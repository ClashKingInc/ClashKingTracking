package scripts

import (
	"context"
	"fmt"
	"sort"

	"clashking_tracking/models"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The SQL identifies active clan snapshots and independently eligible app links.
// Member tags and Town Hall levels come only from the full trackedclans cache.
const trackedPlayerTargetSourcesSQL = `
	SELECT DISTINCT tracked_clan.tag, false AS verified
	FROM server_clans tracked_clan
	JOIN servers server ON server.id = tracked_clan.server_id
	WHERE server.last_command_at >= now() - interval '90 days'
	  AND tracked_clan.tag <> ''
	UNION ALL
	SELECT DISTINCT tag, true AS verified
	FROM player_links
	WHERE is_verified = true
	  AND last_login >= now() - interval '7 days'
	  AND tag <> ''
`

type trackedPlayerTargetSource struct {
	tag      string
	verified bool
}

func loadTrackedPlayerTargets(ctx context.Context, pool *pgxpool.Pool, snapshots trackedClanSnapshotStore, prefix string) ([]models.TrackedPlayerTarget, error) {
	rows, err := pool.Query(ctx, trackedPlayerTargetSourcesSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sources []trackedPlayerTargetSource
	for rows.Next() {
		var source trackedPlayerTargetSource
		if err := rows.Scan(&source.tag, &source.verified); err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return collectTrackedPlayerTargets(ctx, sources, snapshots, prefix)
}

func collectTrackedPlayerTargets(ctx context.Context, sources []trackedPlayerTargetSource, snapshots trackedClanSnapshotStore, prefix string) ([]models.TrackedPlayerTarget, error) {
	byTag := make(map[string]models.TrackedPlayerTarget)
	seenClans := make(map[string]struct{})
	for _, source := range sources {
		if source.tag == "" {
			continue
		}
		if source.verified {
			byTag[source.tag] = models.TrackedPlayerTarget{Tag: source.tag, Verified: true}
			continue
		}
		if _, seen := seenClans[source.tag]; seen {
			continue
		}
		seenClans[source.tag] = struct{}{}
		if snapshots == nil {
			return nil, fmt.Errorf("tracked clan snapshots are unavailable")
		}
		clan, _, ok, err := loadTrackedClanSnapshot[clashy.Clan](ctx, snapshots, prefix, "clan", source.tag)
		if err != nil {
			return nil, fmt.Errorf("load tracked clan snapshot %s: %w", source.tag, err)
		}
		if !ok || clan == nil {
			continue
		}
		for _, member := range clan.Members {
			if member.Tag == "" || member.TownHall < 9 {
				continue
			}
			if _, exists := byTag[member.Tag]; !exists {
				byTag[member.Tag] = models.TrackedPlayerTarget{Tag: member.Tag}
			}
		}
	}
	targets := make([]models.TrackedPlayerTarget, 0, len(byTag))
	for _, target := range byTag {
		targets = append(targets, target)
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].Tag < targets[j].Tag })
	return targets, nil
}
