
import coc
import pendulum as pend
import sentry_sdk

from utility.config import TrackingType
from utility.time import gen_games_season, is_raids

from .tracking import Tracking


class ClanTracker(Tracking):
    """Class to manage clan tracking."""

    def __init__(self):
        super().__init__(tracker_type=TrackingType.BOT_CLAN, batch_size=10_000)
        self.clan_cache: dict[str, coc.Clan] = {}
        self.war_cache = {}

        self.cwl_round_cache = {}
        self.raid_cache: dict[str, coc.RaidLogEntry] = {}
        self.last_private_warlog_warn = {}

        self.reminder_times = [
            f"{int(time)}hr" if time.is_integer() else f"{time}hr" for time in (x * 0.25 for x in range(1, 193))
        ]

    # CLAN CAPITAL
    async def _get_current_raid(self, clan_tag: str):
        """Get the current raid for a clan."""
        try:
            raid_log = await self.coc_client.get_raid_log(clan_tag=clan_tag, limit=1)
            return raid_log[0] if raid_log else None
        except coc.errors.NotFound:
            return None
        except Exception as e:
            self._handle_exception(f"Error fetching current raid for clan {clan_tag}", e)
            return None

    async def _get_previous_raid(self, clan_tag: str):
        """Get the previous raid for a clan from the database."""
        cached_raid = await self.async_mongo.capital_cache.find_one({"tag": clan_tag})
        if cached_raid and "data" in cached_raid:
            return coc.RaidLogEntry(data=cached_raid["data"], client=self.coc_client, clan_tag=clan_tag)
        return None

    async def _process_raid_changes(
        self, clan_tag: str, current_raid: coc.RaidLogEntry, previous_raid: coc.RaidLogEntry
    ):
        """Process changes between the current and previous raids."""
        if previous_raid and current_raid._raw_data == previous_raid._raw_data:
            return  # No changes

        # Update the database with the current raid
        await self.async_mongo.capital_cache.update_one(
            {"tag": clan_tag}, {"$set": {"data": current_raid._raw_data}}, upsert=True
        )

        if previous_raid:
            await self._detect_new_opponents(clan_tag, current_raid, previous_raid)
            await self._detect_raid_state_changes(clan_tag, current_raid, previous_raid)
            await self._detect_member_attacks(clan_tag, current_raid, previous_raid)

        # Update the global clan cache
        self.raid_cache[clan_tag] = current_raid

    async def _detect_new_opponents(
        self, clan_tag: str, current_raid: coc.RaidLogEntry, previous_raid: coc.RaidLogEntry
    ):
        """Detect new offensive opponents and send to Kafka."""
        new_opponents = (
            (clan for clan in current_raid.attack_log if clan not in previous_raid.attack_log)
            if previous_raid.attack_log
            else current_raid.attack_log
        )

        for clan in new_opponents:
            json_data = {
                "type": "new_offensive_opponent",
                "clan": clan._raw_data,
                "clan_tag": clan_tag,
                "raid": current_raid._raw_data,
            }
            self._send_to_kafka("capital", json_data, clan_tag)

    async def _detect_raid_state_changes(
        self, clan_tag: str, current_raid: coc.RaidLogEntry, previous_raid: coc.RaidLogEntry
    ):
        """Detect changes in the raid state and send to Kafka."""
        if current_raid.state != previous_raid.state:
            json_data = {
                "type": "raid_state",
                "clan_tag": current_raid.clan_tag,
                "old_raid": previous_raid._raw_data,
                "raid": current_raid._raw_data,
            }
            self._send_to_kafka("capital", json_data, clan_tag)

    async def _detect_member_attacks(
        self, clan_tag: str, current_raid: coc.RaidLogEntry, previous_raid: coc.RaidLogEntry
    ):
        """Detect member attack changes and send to Kafka."""
        attacked = []
        for member in current_raid.members:
            old_member = coc.utils.get(previous_raid.members, tag=member.tag)
            if old_member is None or old_member.attack_count != member.attack_count:
                attacked.append(member.tag)

        if attacked:
            json_data = {
                "type": "raid_attacks",
                "clan_tag": current_raid.clan_tag,
                "attacked": attacked,
                "raid": current_raid._raw_data,
                "old_raid": previous_raid._raw_data,
            }
            self._send_to_kafka("capital", json_data, clan_tag)

    # CLAN WARS
    async def get_clan_war(self, clan_tag) -> coc.ClanWar | None:
        war = None
        try:
            war = await self.coc_client.get_current_war(clan_tag=clan_tag)
        except coc.errors.PrivateWarLog:
            result = (
                await self.async_mongo.clan_wars.find(
                    {
                        "$and": [
                            {"clans": clan_tag},
                            {"custom_id": None},
                            {"endTime": {"$gte": pend.now(tz=pend.UTC).now()}},
                        ]
                    }
                )
                .sort({"endTime": -1})
                .to_list(length=None)
            )

            if result:
                result = result[0]
                other_clan = result.get("clans", []).remove(clan_tag)
                war = await self.coc_client.get_current_war(clan_tag=other_clan)
        except Exception:
            war = None

        return war

    def _send_reminder(self, time: str, clan_tag: str):
        war = self.war_cache.get(clan_tag)
        if war is None:
            return

        json_data = {"type": "war", "clan_tag": clan_tag, "time": time, "data": war._raw_data}
        self._send_to_kafka("reminder", json_data, clan_tag)

    def _send_cwl_lineup_change(self, previous_war: coc.ClanWar, war: coc.ClanWar):
        clan_members_added = [
            n._raw_data for n in war.clan.members if n.tag not in set(n.tag for n in previous_war.clan.members)
        ]
        clan_members_removed = [
            n._raw_data for n in previous_war.clan.members if n.tag not in set(n.tag for n in war.clan.members)
        ]

        opponent_members_added = [
            n._raw_data for n in war.opponent.members if n.tag not in set(n.tag for n in previous_war.opponent.members)
        ]
        opponent_members_removed = [
            n._raw_data for n in previous_war.opponent.members if n.tag not in set(n.tag for n in war.opponent.members)
        ]

        if opponent_members_added or opponent_members_removed:
            json_data = {
                "type": "cwl_lineup_change",
                "war": war._raw_data,
                "league_group": war.league_group._raw_data,
                "added": clan_members_added,
                "removed": clan_members_removed,
                "opponent_added": opponent_members_added,
                "opponent_removed": opponent_members_removed,
                "clan_tag": war.clan.tag,
                "opponent_tag": war.opponent.tag,
            }
            self._send_to_kafka("war", json_data, war.clan.tag)

    async def _set_reminders(self, war: coc.ClanWar):
        set_times = await self.async_mongo.reminders.distinct(
            "time", filter={"$and": [{"clan": war.clan.tag}, {"type": "War"}]}
        )

        if set_times:
            end_time = pend.instance(war.end_time.time).in_tz("UTC")
            start_time = pend.instance(war.preparation_start_time.time).in_tz("UTC")

            for r_time in reversed(self.reminder_times):
                time_hours = float(r_time.replace("hr", ""))
                time_seconds = int(time_hours * 3600)
                if end_time.diff().in_seconds() >= time_seconds:
                    future_time = end_time.subtract(seconds=time_seconds)
                    if future_time in set_times:
                        self.scheduler.add_job(
                            self._send_reminder,
                            "date",
                            run_date=future_time,
                            args=[r_time, war.clan.tag],
                            id=f"war_end_{war.clan.tag}_{war.opponent.tag}_{future_time.timestamp()}",
                            misfire_grace_time=1200,
                            max_instances=1,
                        )

    async def _handle_war_updates(self, war: coc.ClanWar, clan: coc.Clan):
        # notInWar state, skip
        if not war or war.preparation_start_time is None:
            return

        previous_war = self.war_cache.get(clan.tag)
        self.war_cache[clan.tag] = war

        # this war is different from the previous one (new clans and or/prep time)
        # this means a new war started
        if previous_war != war:
            await self._set_reminders(war=war)
            # if none, no war was tracked before, hit on restarts primarily
            if previous_war is not None:
                json_data = {
                    "type": "new_war",
                    "new_war": war._raw_data,
                    "new_league_group": war.league_group._raw_data if war.is_cwl else None,
                    "clan_tag": war.clan.tag,
                }
                self._send_to_kafka("war", json_data, war.clan.tag)
            return

        # if the war is a cwl war, there is special things we need to do
        if war.is_cwl:
            war_tags = war.league_group._raw_data.get("rounds")
            # if the war is in prep (only can happen for first war really) or it is in the last round, ignore
            if war.state == coc.enums.WarState.preparation or war.war_tag in war_tags[-1]:
                cwl_round = None
            else:
                cwl_round = await self.coc_client.get_current_war(
                    clan_tag=clan.tag, cwl_round=coc.WarRound.current_preparation
                )

            if cwl_round:
                previous_cwl_round = self.cwl_round_cache.get(clan.tag)
                self.cwl_round_cache[clan.tag] = cwl_round
                # if it is null, ignore, nothing we can do with it, first time it has popped up
                # other wise check for lineup changes
                if previous_cwl_round is not None:
                    self._send_cwl_lineup_change(previous_cwl_round, cwl_round)

        if new_attacks := [a for a in war.attacks if a not in set(previous_war.attacks)]:
            json_data = {
                "type": "new_attacks",
                "war": war._raw_data,
                "league_group": war.league_group._raw_data if war.is_cwl else None,
                "attacks": [attack._raw_data for attack in new_attacks],
                "clan_tag": clan.tag,
            }
            self._send_to_kafka("war", json_data, clan.tag)

        if war.state != previous_war.state:
            json_data = {
                "type": "war_state",
                "old_war": previous_war._raw_data if previous_war else None,
                "new_war": war._raw_data,
                "previous_league_group": previous_war.league_group._raw_data if previous_war.is_cwl else None,
                "new_league_group": war.league_group._raw_data if war.is_cwl else None,
                "clan_tag": clan.tag,
            }
            self._send_to_kafka("war", json_data, clan.tag)

    # CLAN UPDATES
    def _handle_private_warlog(self, clan):
        """Handle cases where the war log is private."""
        now = pend.now(tz=pend.UTC)
        last_warn = self.last_private_warlog_warn.get(clan.tag)

        if not clan.public_war_log:
            if last_warn is None or (now - last_warn).total_seconds() >= 12 * 3600:
                self.last_private_warlog_warn[clan.tag] = now
                json_data = {"type": "war_log_closed", "clan": clan._raw_data}
                self._send_to_kafka("clan", json_data, clan.tag)

    def _handle_attribute_changes(self, clan, previous_clan):
        """Handle changes in clan attributes."""
        attributes = [
            "level",
            "type",
            "description",
            "location",
            "capital_league",
            "required_townhall",
            "required_trophies",
            "war_win_streak",
            "war_league",
            "member_count",
        ]
        changed_attributes = [attr for attr in attributes if getattr(clan, attr) != getattr(previous_clan, attr)]

        if changed_attributes:
            json_data = {"types": changed_attributes, "old_clan": previous_clan._raw_data, "new_clan": clan._raw_data}
            self._send_to_kafka("clan", json_data, clan.tag)

    def _handle_member_changes(self, clan, previous_clan):
        """Handle changes in clan membership."""
        current_members = clan.members_dict
        previous_members = previous_clan.members_dict

        members_joined = [n._raw_data for n in clan.members if n.tag not in previous_members]
        members_left = [m._raw_data for m in previous_clan.members if m.tag not in current_members]

        if members_joined or members_left:
            json_data = {
                "type": "members_join_leave",
                "old_clan": previous_clan._raw_data,
                "new_clan": clan._raw_data,
                "joined": members_joined,
                "left": members_left,
            }
            self._send_to_kafka("clan", json_data, clan.tag)

    def _handle_donation_updates(self, clan, previous_clan):
        """Handle updates to member donations."""
        previous_donations = {n.tag: (n.donations, n.received) for n in previous_clan.members}

        for member in clan.members:
            if (donated := previous_donations.get(member.tag)) is not None:
                mem_donos, mem_received = donated
                if mem_donos < member.donations or mem_received < member.received:
                    json_data = {
                        "type": "all_member_donations",
                        "old_clan": previous_clan._raw_data,
                        "new_clan": clan._raw_data,
                    }
                    self._send_to_kafka("clan", json_data, clan.tag)
                    break

    def is_member_eligible(self, member: coc.ClanMember, roles, townhall_levels):
        """Check if a member meets the role and townhall level criteria."""
        return (not roles or str(member.role) in roles) and (
            not townhall_levels or str(member.town_hall) in townhall_levels
        )

    # REMINDERS
    async def _send_clan_games_reminders(self):
        now = pend.now(tz=pend.UTC)
        clan_games_end_time = now.start_of('month').add(days=28, hours=9)
        remaining_hours = (clan_games_end_time - now).in_hours()

        if remaining_hours <= 0:
            self.logger.info('Clan Games have ended. No reminders to send.')
            return

        remaining_time_formatted = f'{int(remaining_hours)} hr'

        reminders = await self.async_mongo.reminders.find({'type': 'Clan Games', 'time': remaining_time_formatted})
        reminders = await reminders.to_list(length=None)

        for reminder in reminders:
            clan_tag = reminder.get('clan')
            clan = self.clan_cache.get(clan_tag)
            if not clan:
                continue

            point_threshold = reminder.get('point_threshold', 0)
            roles = reminder.get('roles', [])
            townhall_levels = reminder.get('townhalls', [])

            eligible_members = [
                member for member in clan.members if self.is_member_eligible(member, roles, townhall_levels)
            ]

            player_points = await self.async_mongo.new_player_stats.find(
                {"clan_tag": clan_tag, "season": gen_games_season()}
            )

            missing_clan_members = []
            for member in eligible_members:
                db_member = next((m for m in player_points if m['tag'] == member.tag), None)
                points = db_member['clan_games'] if db_member else 0

                if points <= point_threshold:
                    missing_clan_members.append(
                        {'name': member.name, 'townhall': member.town_hall, 'role': member.role, 'points': points}
                    )

            if missing_clan_members:
                reminder_data = {
                    "clan_data": clan._raw_data,
                    "reminder_data": reminder,
                    "missing": missing_clan_members,
                }
                self._send_to_kafka(topic="reminders", key=clan_tag, data=reminder_data)

    async def _send_inactivity_reminders(self):
        reminders = await self.async_mongo.reminders.find({'type': 'inactivity'})
        reminders = await reminders.to_list(length=None)

        for reminder in reminders:
            inactive_threshold = int(reminder['time'].replace(' hr', '')) * 3600
            clan_tag = reminder['clan']
            clan = self.clan_cache.get(clan_tag)
            if not clan:
                continue

            now = pend.now('UTC')
            lower_bound = now.subtract(seconds=inactive_threshold + 3600 - 1)
            upper_bound = now.subtract(seconds=inactive_threshold)

            # Fetch all members from the database asynchronously
            inactive_members = await self.async_mongo.base_player.find(
                {
                    'tag': {'$in': [member.tag for member in clan.members]},
                    'last_online': {'$gte': lower_bound.int_timestamp, '$lt': upper_bound.int_timestamp},
                }
            )
            inactive_members = await inactive_members.to_list(length=None)

            clan_inactive_members = []
            for member in inactive_members:
                clan_member = clan.get_member(tag=member['tag'])
                if not clan_member:
                    continue
                clan_inactive_members.append(
                    {
                        "name": clan_member.name,
                        "tag": clan_member.tag,
                        "townhall": clan_member.town_hall,
                        "last_online": member['last_online'],
                    }
                )

            if clan_inactive_members:
                reminder_data = {
                    "clan_data": clan._raw_data,
                    "reminder_data": reminder,
                    "missing": clan_inactive_members,
                }
                self._send_to_kafka(topic="reminders", key=clan_tag, data=reminder_data)

            return inactive_members

    async def _send_raid_reminders(self):
        now = pend.now(tz=pend.UTC)
        raid_end_time = now.start_of('week').add(days=7, hours=7)  # Monday 7:00 UTC
        remaining_hours = (raid_end_time - now).in_hours()

        if remaining_hours <= 0:
            self.logger.info('Raids have ended. No reminders to send.')
            return

        remaining_time_formatted = f'{int(remaining_hours)} hr'

        reminders = await self.async_mongo.reminders.find({'type': 'Clan Capital', 'time': remaining_time_formatted})
        reminders = await reminders.to_list(length=None)

        for reminder in reminders:
            clan_tag = reminder.get('clan')
            clan = self.clan_cache.get(clan_tag)
            if not clan:
                continue

            attack_threshold = reminder.get('attack_threshold', 0)
            roles = reminder.get('roles', [])
            townhall_levels = reminder.get('townhalls', [])

            raid_log = self.raid_cache.get(clan.tag)
            if not raid_log:
                continue

            missing_clan_members = []

            for member in raid_log.members:
                attack_total = member.attack_limit + member.bonus_attack_limit
                if int(member.attack_count) < (int(attack_total) - int(attack_threshold)):
                    enriched_member = clan.get_member(tag=member.tag)
                    if enriched_member and self.is_member_eligible(enriched_member, roles, townhall_levels):
                        missing_clan_members.append(
                            {
                                "name": enriched_member.name,
                                "townhall": enriched_member.town_hall,
                                "role": enriched_member.role,
                                "attacks": member.attack_count,
                                "total_attacks": attack_total,
                            }
                        )

            for member in clan.members:
                raid_member = raid_log.get_member(tag=member.tag)
                if raid_member and self.is_member_eligible(member, roles, townhall_levels):
                    missing_clan_members.append(
                        {
                            "name": member.name,
                            "townhall": member.town_hall,
                            "role": member.role,
                            "attacks": 0,
                            "total_attacks": 5,
                        }
                    )

            if missing_clan_members:
                reminder_data = {
                    "clan_data": clan._raw_data,
                    "reminder_data": reminder,
                    "raid_data": raid_log._raw_data,
                    "missing": missing_clan_members,
                }
                self._send_to_kafka(topic="reminders", key=clan_tag, data=reminder_data)

    # PROCESSING
    def clan_list(self) -> list[str]:
        return self.mongo.clans_db.distinct("tag")

    async def _track_clan(self, clan_tag):
        """Track updates for a specific clan."""
        sentry_sdk.set_context("clan_tracking", {"clan_tag": clan_tag})
        clan = None
        try:
            clan = await self.coc_client.get_clan(tag=clan_tag)
        except coc.errors.NotFound:
            # REMOVE CLAN FROM TRACKING
            self._handle_exception(f"Clan {clan_tag} not found", None)
        except Exception as e:
            self._handle_exception(f"Error fetching clan {clan_tag}", e)

        if not clan:
            return

        previous_clan = self.clan_cache.get(clan.tag)
        self.clan_cache[clan.tag] = clan

        if previous_clan is None:
            return

        sentry_sdk.add_breadcrumb(message=f"Tracking clan: {clan_tag}", level="info")
        self._handle_private_warlog(clan)
        self._handle_attribute_changes(clan, previous_clan)
        self._handle_member_changes(clan, previous_clan)
        self._handle_donation_updates(clan, previous_clan)

        war = await self.get_clan_war(clan.tag)
        await self._handle_war_updates(war=war, clan=clan)

        if is_raids():
            current_raid = await self._get_current_raid(clan_tag)
            if current_raid:
                previous_raid = await self._get_previous_raid(clan_tag)
                await self._process_raid_changes(clan_tag, current_raid, previous_raid)

    async def run(self):
        import time

        await self.initialize()
        while True:
            t = time.time()
            tasks = []
            for clan_tag in self.clan_list():
                tasks.append(self._track_clan(clan_tag=clan_tag))
            await self._batch_tasks(tasks=tasks)

            if not is_raids():
                self.raid_cache.clear()

            print(f"Finished tracking clans in {time.time() - t} seconds")
