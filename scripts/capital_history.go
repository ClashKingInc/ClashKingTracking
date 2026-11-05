package scripts

import (
	"context"
	"encoding/json"
	"fmt"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

func storeCapitalRaidHistory(ctx context.Context, pool *pgxpool.Pool, tag string, raids []clashy.RaidLogEntry, observed time.Time) error {
	for _, raid := range raids {
		if raid.StartTime == nil || raid.EndTime == nil || !raid.EndTime.Time.After(raid.StartTime.Time) || (raid.State != "ongoing" && raid.State != "ended") {
			continue
		}
		raw, err := json.Marshal(raid)
		if err != nil {
			return fmt.Errorf("encode raid history: %w", err)
		}
		// Persist before the short-lived cache fast path. An unchanged cached raid
		// must still seed history after the feature is deployed. Final snapshots
		// cannot be overwritten by a delayed ongoing observation.
		_, err = pool.Exec(ctx, `INSERT INTO capital_raid_history(clan_tag,start_time,end_time,state,data,observed_at)
    VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(clan_tag,start_time) DO UPDATE
    SET end_time=EXCLUDED.end_time,state=EXCLUDED.state,data=EXCLUDED.data,observed_at=EXCLUDED.observed_at
    WHERE capital_raid_history.observed_at<=EXCLUDED.observed_at
      AND (capital_raid_history.state<>'ended' OR EXCLUDED.state='ended')
      AND capital_raid_history.data IS DISTINCT FROM EXCLUDED.data`, tag, raid.StartTime.Time.UTC(), raid.EndTime.Time.UTC(), raid.State, raw, observed)
		if err != nil {
			return err
		}
	}
	return nil
}
