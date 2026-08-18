package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
)

const remindersDomainName = "reminders"

type remindersDomain struct {
	pool *pgxpool.Pool
}

const discordReminderPayloadSQL = `jsonb_build_object(
	'id', reminder.id::text,
	'server_id', reminder.server_id,
	'type_name', reminder.type_name,
	'clan_tag', reminder.clan_tag,
	'channel_id', COALESCE(reminder.channel_id, ''),
	'trigger_time', COALESCE(reminder.trigger_time, ''),
	'minutes_remaining', reminder.minutes_remaining,
	'custom_text', reminder.custom_text,
	'town_halls', COALESCE(reminder.townhalls, '{}'::integer[]),
	'roles', reminder.roles,
	'war_types', reminder.war_type_names,
	'trigger_threshold', COALESCE(reminder.trigger_threshold, 0)
)`

func NewRemindersDomain() platform.Domain { return &remindersDomain{} }
func (d *remindersDomain) Name() string   { return remindersDomainName }

func (d *remindersDomain) Run(ctx context.Context, app *platform.App) error {
	if app.Config.TimescaleURL == "" || app.Valkey == nil || app.Clash == nil {
		return errors.New("reminders requires Timescale, Valkey, and the Clash API")
	}
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return err
	}
	d.pool = pool
	defer pool.Close()
	if err := d.reconcileAllWars(ctx); err != nil {
		return err
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 5)
	for _, run := range []func(context.Context, *platform.App) error{
		d.runEventLoop,
		d.runDueWarJobs,
		d.runMobileWarReconciliation,
		d.runRaidReminderClock,
		d.runFixedDiscordReminderClock,
	} {
		wg.Add(1)
		go func(run func(context.Context, *platform.App) error) {
			defer wg.Done()
			errCh <- run(ctx, app)
		}(run)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (d *remindersDomain) runFixedDiscordReminderClock(ctx context.Context, app *platform.App) error {
	for {
		now := time.Now().UTC()
		next := now.Truncate(15 * time.Minute).Add(15 * time.Minute)
		if err := sleepOrDone(ctx, next.Sub(now)); err != nil {
			return err
		}
		now = time.Now().UTC()
		if err := d.sendClanGamesReminderInterval(ctx, app, now); err != nil {
			app.Logger.Error("clan games reminder interval failed", "err", err)
		}
		if err := d.sendInactivityReminderInterval(ctx, app, now); err != nil {
			app.Logger.Error("inactivity reminder interval failed", "err", err)
		}
	}
}

func (d *remindersDomain) sendClanGamesReminderInterval(ctx context.Context, app *platform.App, now time.Time) error {
	start := time.Date(now.Year(), now.Month(), 22, 8, 0, 0, 0, time.UTC)
	end := time.Date(now.Year(), now.Month(), 28, 8, 0, 0, 0, time.UTC)
	if now.Before(start) || !now.Before(end) {
		return nil
	}
	minutes := int(end.Sub(now).Round(time.Minute) / time.Minute)
	rows, err := d.pool.Query(ctx, `
		SELECT reminder.clan_tag, `+discordReminderPayloadSQL+`,
		       COALESCE(jsonb_agg(jsonb_build_object(
		           'name', member->>'name', 'tag', member->>'tag',
		           'townhall', COALESCE((member->>'town_hall')::int, 0),
		           'points', COALESCE(points.value, 0)
		       )) FILTER (WHERE member->>'tag' IS NOT NULL), '[]'::jsonb)
		FROM reminders reminder
		JOIN basic_clan clan ON clan.tag = reminder.clan_tag
		CROSS JOIN LATERAL jsonb_array_elements(clan.members) member
		LEFT JOIN LATERAL (
			SELECT COALESCE(sum(change.delta), 0)::bigint AS value
			FROM player_stat_changes change
			WHERE change.player_tag = member->>'tag' AND change.stat_type = 'clan_games'
			  AND change.event_time >= $2
		) points ON true
		WHERE reminder.type_name = 'Clan Games' AND reminder.minutes_remaining = $1
		  AND COALESCE(points.value, 0) <= COALESCE(reminder.trigger_threshold, 0)
		GROUP BY reminder.id, reminder.clan_tag
	`, minutes, start)
	if err != nil {
		return err
	}
	defer rows.Close()
	return d.publishFixedReminderRows(ctx, app, rows, "clan_games")
}

func (d *remindersDomain) sendInactivityReminderInterval(ctx context.Context, app *platform.App, now time.Time) error {
	rows, err := d.pool.Query(ctx, inactivityReminderRowsSQL, now)
	if err != nil {
		return err
	}
	defer rows.Close()
	return d.publishFixedReminderRows(ctx, app, rows, "inactivity")
}

const inactivityReminderRowsSQL = `
		SELECT reminder.clan_tag, ` + discordReminderPayloadSQL + `,
		       COALESCE(jsonb_agg(jsonb_build_object(
		           'name', member->>'name', 'tag', member->>'tag',
		           'townhall', COALESCE((member->>'town_hall')::int, 0),
		           'last_online', extract(epoch FROM online.last_seen)::bigint
		       )) FILTER (WHERE online.last_seen IS NOT NULL), '[]'::jsonb)
		FROM reminders reminder
		JOIN basic_clan clan ON clan.tag = reminder.clan_tag
		CROSS JOIN LATERAL jsonb_array_elements(clan.members) member
		LEFT JOIN LATERAL (
			SELECT max(event.seen_at) AS last_seen
			FROM player_online_events event WHERE event.tag = member->>'tag'
		) online ON true
		WHERE lower(reminder.type_name) = 'inactivity'
		  AND online.last_seen >= $1::timestamptz - make_interval(mins => reminder.minutes_remaining + 15)
		  AND online.last_seen <  $1::timestamptz - make_interval(mins => reminder.minutes_remaining)
		GROUP BY reminder.id, reminder.clan_tag
`

type reminderRows interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func (d *remindersDomain) publishFixedReminderRows(ctx context.Context, app *platform.App, rows reminderRows, reminderType string) error {
	for rows.Next() {
		var clanTag string
		var reminderRaw, missingRaw []byte
		if err := rows.Scan(&clanTag, &reminderRaw, &missingRaw); err != nil {
			return err
		}
		var reminderData map[string]any
		var missing []map[string]any
		if json.Unmarshal(reminderRaw, &reminderData) != nil || json.Unmarshal(missingRaw, &missing) != nil || len(missing) == 0 {
			continue
		}
		clan, err := platform.RetryClashFetch(ctx, app.Availability, func(fetchCtx context.Context) (*clashy.Clan, error) {
			return app.Clash.GetClan(fetchCtx, clanTag)
		})
		if err != nil || clan == nil {
			continue
		}
		roles := reminderStringList(reminderData["roles"])
		townHalls := reminderIntList(reminderData["town_halls"])
		members := make(map[string]clashy.ClanMember, len(clan.Members))
		for _, member := range clan.Members {
			members[member.Tag] = member
		}
		filtered := missing[:0]
		for _, item := range missing {
			tag, _ := item["tag"].(string)
			member, ok := members[tag]
			if !ok || !clanMemberEligible(member, roles, townHalls) {
				continue
			}
			item["role"] = member.Role
			item["townhall"] = member.TownHall
			filtered = append(filtered, item)
		}
		missing = filtered
		if len(missing) == 0 {
			continue
		}
		if err := app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: clanTag, Value: map[string]any{
			"type": reminderType, "clan": clan, "reminder": reminderData, "members": missing,
		}}); err != nil {
			return err
		}
	}
	return rows.Err()
}

func reminderStringList(value any) []string {
	items, _ := value.([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok {
			out = append(out, text)
		}
	}
	return out
}

func reminderIntList(value any) []int {
	items, _ := value.([]any)
	out := make([]int, 0, len(items))
	for _, item := range items {
		if number, ok := item.(float64); ok {
			out = append(out, int(number))
		}
	}
	return out
}

func (d *remindersDomain) reconcileAllWars(ctx context.Context) error {
	rows, err := d.pool.Query(ctx, `SELECT schedule_key FROM war_schedule WHERE end_time > now()`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return err
		}
		if err := d.reconcileWar(ctx, key); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (d *remindersDomain) reconcileWar(ctx context.Context, scheduleKey string) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `
		WITH schedule AS (
			SELECT schedule_key, source_clan_tag, opponent_tag, end_time, war_type
			FROM war_schedule WHERE schedule_key = $1 AND end_time > now()
		), required_offsets AS (
			SELECT DISTINCT reminder.minutes_remaining
			FROM schedule
			JOIN reminders reminder
			  ON reminder.type_name = 'War'
			 AND reminder.clan_tag IN (schedule.source_clan_tag, schedule.opponent_tag)
			WHERE reminder.minutes_remaining > 0
			  AND (cardinality(reminder.war_type_names) = 0 OR schedule.war_type = ANY(reminder.war_type_names))
			UNION
			SELECT DISTINCT timing
			FROM schedule
			JOIN player_timers timer ON timer.event_type = 'war' AND timer.event_key = schedule.schedule_key
			JOIN mobile_notification_accounts account ON account.player_tag = timer.player_tag AND account.active = true AND account.source = 'verified'
			JOIN mobile_push_devices device ON device.user_id = account.user_id
			CROSS JOIN LATERAL unnest(device.reminder_timings) timing
			WHERE device.enabled = true AND device.provider = 'fcm' AND device.war_reminders_enabled = true
		)
		INSERT INTO war_reminder_jobs (schedule_key, minutes_remaining, run_at)
		SELECT schedule.schedule_key, required.minutes_remaining,
		       schedule.end_time - make_interval(mins => required.minutes_remaining)
		FROM schedule
		JOIN required_offsets required ON true
		WHERE schedule.end_time - make_interval(mins => required.minutes_remaining) > now()
		ON CONFLICT (schedule_key, minutes_remaining) DO UPDATE SET run_at = EXCLUDED.run_at
	`, scheduleKey)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		DELETE FROM war_reminder_jobs job
		WHERE job.schedule_key = $1
		  AND NOT EXISTS (
			SELECT 1 FROM reminders reminder
			JOIN war_schedule schedule ON schedule.schedule_key = job.schedule_key
			WHERE reminder.type_name = 'War'
			  AND reminder.clan_tag IN (schedule.source_clan_tag, schedule.opponent_tag)
			  AND reminder.minutes_remaining = job.minutes_remaining
			  AND (cardinality(reminder.war_type_names) = 0 OR schedule.war_type = ANY(reminder.war_type_names))
		  )
		  AND NOT EXISTS (
			SELECT 1
			FROM player_timers timer
			JOIN mobile_notification_accounts account ON account.player_tag = timer.player_tag AND account.active = true AND account.source = 'verified'
			JOIN mobile_push_devices device ON device.user_id = account.user_id
			WHERE timer.event_type = 'war' AND timer.event_key = job.schedule_key
			  AND device.enabled = true AND device.provider = 'fcm' AND device.war_reminders_enabled = true
			  AND job.minutes_remaining = ANY(device.reminder_timings)
		  )
	`, scheduleKey)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (d *remindersDomain) runMobileWarReconciliation(ctx context.Context, _ *platform.App) error {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			rows, err := d.pool.Query(ctx, `
				SELECT DISTINCT timer.event_key
				FROM player_timers timer
				JOIN mobile_notification_accounts account ON account.player_tag = timer.player_tag AND account.active = true AND account.source = 'verified'
				JOIN mobile_push_devices device ON device.user_id = account.user_id
				WHERE timer.event_type = 'war' AND timer.expires_at > now()
				  AND device.enabled = true AND device.provider = 'fcm' AND device.war_reminders_enabled = true
			`)
			if err != nil {
				return err
			}
			var keys []string
			for rows.Next() {
				var key string
				if err := rows.Scan(&key); err != nil {
					rows.Close()
					return err
				}
				keys = append(keys, key)
			}
			rows.Close()
			for _, key := range keys {
				if err := d.reconcileWar(ctx, key); err != nil {
					return err
				}
			}
		}
	}
}

func (d *remindersDomain) runEventLoop(ctx context.Context, app *platform.App) error {
	group := "reminders"
	err := app.Valkey.Do(ctx, app.Valkey.B().XgroupCreate().Key(app.Config.EventStreamName).Group(group).Id("0").Mkstream().Build()).Error()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	consumer := app.Config.EventStreamConsumer + ":reminders"
	for {
		result, err := app.Valkey.Do(ctx, app.Valkey.B().Xreadgroup().Group(group, consumer).Count(50).Block(5000).
			Streams().Key(app.Config.EventStreamName).Id(">").Build()).AsXRead()
		if err != nil {
			if valkey.IsValkeyNil(err) {
				continue
			}
			return err
		}
		for _, entry := range result[app.Config.EventStreamName] {
			topic := entry.FieldValues["topic"]
			clanTag := entry.FieldValues["clan_tag"]
			var value map[string]any
			if err := json.Unmarshal([]byte(entry.FieldValues["value"]), &value); err != nil {
				return err
			}
			if err := d.handleReconciliationEvent(ctx, topic, clanTag, value); err != nil {
				return err
			}
			if err := app.Valkey.Do(ctx, app.Valkey.B().Xack().Key(app.Config.EventStreamName).Group(group).Id(entry.ID).Build()).Error(); err != nil {
				return err
			}
		}
	}
}

func (d *remindersDomain) handleReconciliationEvent(ctx context.Context, topic, clanTag string, value map[string]any) error {
	switch topic {
	case "war_schedule":
		key, _ := value["schedule_key"].(string)
		if key == "" {
			return errors.New("war_schedule event requires schedule_key")
		}
		return d.reconcileWar(ctx, key)
	case "reminder_config":
		if clanTag == "" {
			return errors.New("reminder_config event requires clan_tag")
		}
		rows, err := d.pool.Query(ctx, `
			SELECT schedule_key
			FROM war_schedule
			WHERE end_time > now()
			  AND $1 IN (source_clan_tag, opponent_tag)
		`, clanTag)
		if err != nil {
			return err
		}
		defer rows.Close()
		var keys []string
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				return err
			}
			keys = append(keys, key)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, key := range keys {
			if err := d.reconcileWar(ctx, key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *remindersDomain) runDueWarJobs(ctx context.Context, app *platform.App) error {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		rows, err := d.pool.Query(ctx, `
			SELECT job.schedule_key, job.minutes_remaining, schedule.source_clan_tag,
			       COALESCE(schedule.war_tag, '')
			FROM war_reminder_jobs job
			JOIN war_schedule schedule ON schedule.schedule_key = job.schedule_key
			WHERE job.run_at <= now()
			ORDER BY job.run_at
			LIMIT 100
		`)
		if err != nil {
			return err
		}
		type dueJob struct {
			key             string
			minutes         int
			clanTag, warTag string
		}
		var jobs []dueJob
		for rows.Next() {
			var job dueJob
			if err := rows.Scan(&job.key, &job.minutes, &job.clanTag, &job.warTag); err != nil {
				rows.Close()
				return err
			}
			jobs = append(jobs, job)
		}
		rows.Close()
		for _, job := range jobs {
			if err := app.Availability.Wait(ctx); err != nil {
				return err
			}
			var stillDue bool
			if err := d.pool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM war_reminder_jobs
					WHERE schedule_key = $1 AND minutes_remaining = $2 AND run_at <= now()
				)
			`, job.key, job.minutes).Scan(&stillDue); err != nil {
				return err
			}
			if !stillDue {
				continue
			}
			var war *clashy.ClanWar
			if job.warTag != "" {
				wars, fetchErr := platform.RetryClashFetch(ctx, app.Availability, func(fetchCtx context.Context) ([]clashy.ClanWar, error) {
					return app.Clash.GetLeagueWars(fetchCtx, []string{job.warTag})
				})
				if fetchErr == nil && len(wars) > 0 {
					war = &wars[0]
				}
			} else {
				war, _ = platform.RetryClashFetch(ctx, app.Availability, func(fetchCtx context.Context) (*clashy.ClanWar, error) {
					return app.Clash.GetClanWar(fetchCtx, job.clanTag)
				})
			}
			if war != nil && war.EndTime != nil && war.EndTime.Time.After(time.Now().UTC()) {
				raw, _ := json.Marshal(war)
				_ = app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: job.clanTag, Value: map[string]any{
					"type": "war", "schedule_key": job.key, "minutes_remaining": job.minutes,
					"data": json.RawMessage(raw),
				}})
			}
			_, _ = d.pool.Exec(ctx, `DELETE FROM war_reminder_jobs WHERE schedule_key = $1 AND minutes_remaining = $2`, job.key, job.minutes)
		}
	}
}

func (d *remindersDomain) runRaidReminderClock(ctx context.Context, app *platform.App) error {
	for {
		now := time.Now().UTC()
		next := now.Truncate(15 * time.Minute).Add(15 * time.Minute)
		if err := sleepOrDone(ctx, next.Sub(now)); err != nil {
			return err
		}
		if !capitalWeekendActive(time.Now().UTC()) {
			continue
		}
		if err := d.sendRaidReminderInterval(ctx, app, time.Now().UTC()); err != nil {
			app.Logger.Error("raid reminder interval failed", "err", err)
		}
	}
}

func (d *remindersDomain) sendRaidReminderInterval(ctx context.Context, app *platform.App, now time.Time) error {
	end := raidWeekendEnd(now)
	minutes := int(end.Sub(now).Round(time.Minute) / time.Minute)
	if minutes <= 0 || minutes%15 != 0 {
		return nil
	}
	if err := d.sendDiscordRaidReminders(ctx, app, minutes); err != nil {
		return err
	}
	rows, err := d.pool.Query(ctx, `
		SELECT DISTINCT account.user_id, account.player_tag
		FROM mobile_notification_accounts account
		JOIN mobile_push_devices device ON device.user_id = account.user_id
		WHERE account.active = true AND account.source = 'verified'
		  AND device.enabled = true AND device.provider = 'fcm'
		  AND device.raid_reminders_enabled = true
		  AND $1 = ANY(device.raid_reminder_timings)
	`, minutes)
	if err != nil {
		return err
	}
	type account struct{ userID, tag string }
	var accounts []account
	var tags []string
	for rows.Next() {
		var item account
		if err := rows.Scan(&item.userID, &item.tag); err != nil {
			rows.Close()
			return err
		}
		accounts = append(accounts, item)
		tags = append(tags, item.tag)
	}
	rows.Close()
	if len(tags) == 0 {
		return nil
	}
	clanValues, err := app.Valkey.Do(ctx, app.Valkey.B().Hmget().Key(verifiedPlayerClanHashKey).Field(tags...).Build()).ToArray()
	if err != nil {
		return err
	}
	type grouped struct {
		userID, clanTag string
		tags            []string
	}
	groups := make(map[string]*grouped)
	for i, item := range accounts {
		if i >= len(clanValues) {
			break
		}
		clanTag, valueErr := clanValues[i].ToString()
		if valueErr != nil || clanTag == "" {
			continue
		}
		key := item.userID + "\x00" + clanTag
		if groups[key] == nil {
			groups[key] = &grouped{userID: item.userID, clanTag: clanTag}
		}
		groups[key].tags = append(groups[key].tags, item.tag)
	}
	for _, group := range groups {
		raid, ok := loadCachedRaid(ctx, app, group.clanTag)
		if !ok {
			var fetchErr error
			raid, ok, fetchErr = fetchReminderRaid(ctx, app, group.clanTag)
			if fetchErr != nil || !ok {
				continue
			}
		}
		members := make(map[string]clashy.RaidMember, len(raid.Members))
		for _, member := range raid.Members {
			members[member.Tag] = member
		}
		remaining := remainingRaidAttacks(group.tags, members)
		if remaining > 0 {
			_ = app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: group.clanTag, Value: map[string]any{
				"type": "raid_mobile", "user_id": group.userID, "minutes_remaining": minutes,
				"remaining_attacks": remaining, "raid_end": end.Format(time.RFC3339),
			}})
		}
	}
	return nil
}

func remainingRaidAttacks(tags []string, members map[string]clashy.RaidMember) int {
	remaining := 0
	for _, tag := range tags {
		member, found := members[tag]
		if !found {
			remaining += 5
			continue
		}
		limit := member.AttackLimit + member.BonusAttackLimit
		if limit == 0 {
			limit = 5
		}
		if unused := limit - member.AttackCount; unused > 0 {
			remaining += unused
		}
	}
	return remaining
}

func (d *remindersDomain) sendDiscordRaidReminders(ctx context.Context, app *platform.App, minutes int) error {
	rows, err := d.pool.Query(ctx, `
		SELECT id::text, server_id, clan_tag, COALESCE(channel_id, ''),
		       COALESCE(trigger_time, ''), minutes_remaining, custom_text,
		       COALESCE(townhalls, '{}'::integer[]), roles, war_type_names,
		       COALESCE(trigger_threshold, 1)
		FROM reminders
		WHERE type_name = 'Clan Capital' AND minutes_remaining = $1
		ORDER BY clan_tag, id
	`, minutes)
	if err != nil {
		return err
	}
	defer rows.Close()
	byClan := make(map[string][]raidReminder)
	for rows.Next() {
		var reminder raidReminder
		if err := rows.Scan(&reminder.ID, &reminder.ServerID, &reminder.ClanTag, &reminder.ChannelID,
			&reminder.TriggerTime, &reminder.MinutesRemaining, &reminder.CustomText, &reminder.TownHalls,
			&reminder.Roles, &reminder.WarTypes, &reminder.AttackThreshold); err != nil {
			return err
		}
		byClan[reminder.ClanTag] = append(byClan[reminder.ClanTag], reminder)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for clanTag, reminders := range byClan {
		raid, ok := loadCachedRaid(ctx, app, clanTag)
		if !ok {
			var fetchErr error
			raid, ok, fetchErr = fetchReminderRaid(ctx, app, clanTag)
			if fetchErr != nil || !ok {
				continue
			}
		}
		clan, fetchErr := fetchReminderClan(ctx, app, clanTag)
		if fetchErr != nil || clan == nil {
			continue
		}
		for _, reminder := range reminders {
			missing := raidMissingMembers(*clan, raid, reminder)
			if len(missing) == 0 {
				continue
			}
			if err := app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: clanTag, Value: map[string]any{
				"type": "raid", "clan": clan, "raid": &raid,
				"reminder": reminder.eventData(), "members": missing,
			}}); err != nil {
				return err
			}
		}
	}
	return nil
}

func fetchReminderRaid(ctx context.Context, app *platform.App, clanTag string) (clashy.RaidLogEntry, bool, error) {
	entries, err := platform.RetryClashFetch(ctx, app.Availability, func(fetchCtx context.Context) ([]clashy.RaidLogEntry, error) {
		start := time.Now()
		entries, fetchErr := app.Clash.GetRaidLog(fetchCtx, clanTag, clashy.PageOptions{Limit: 1})
		app.Stats.RecordRequest(remindersDomainName, time.Since(start), fetchErr)
		return entries, fetchErr
	})
	if err != nil || len(entries) == 0 {
		return clashy.RaidLogEntry{}, false, err
	}
	return entries[0], true, nil
}

func fetchReminderClan(ctx context.Context, app *platform.App, clanTag string) (*clashy.Clan, error) {
	return platform.RetryClashFetch(ctx, app.Availability, func(fetchCtx context.Context) (*clashy.Clan, error) {
		start := time.Now()
		clan, err := app.Clash.GetClan(fetchCtx, clanTag)
		app.Stats.RecordRequest(remindersDomainName, time.Since(start), err)
		return clan, err
	})
}

func loadCachedRaid(ctx context.Context, app *platform.App, clanTag string) (clashy.RaidLogEntry, bool) {
	raw, err := app.Valkey.Do(ctx, app.Valkey.B().Get().Key(capitalCacheKey(app.Config.CapitalSnapshotPrefix, clanTag)).Build()).AsBytes()
	if err != nil {
		return clashy.RaidLogEntry{}, false
	}
	decoded, err := decompressRaidPayload(raw)
	if err != nil {
		return clashy.RaidLogEntry{}, false
	}
	var raid clashy.RaidLogEntry
	if json.Unmarshal(decoded, &raid) != nil {
		return clashy.RaidLogEntry{}, false
	}
	return raid, true
}

func raidWeekendEnd(now time.Time) time.Time {
	now = now.UTC()
	days := (int(time.Monday) - int(now.Weekday()) + 7) % 7
	end := time.Date(now.Year(), now.Month(), now.Day(), 7, 0, 0, 0, time.UTC).AddDate(0, 0, days)
	if !end.After(now) {
		end = end.AddDate(0, 0, 7)
	}
	return end
}
