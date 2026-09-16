//go:build script_internal_tests

package scripts

import (
	"context"
	"strings"
	"testing"
	"time"

	"clashking_tracking/models"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type compositionScopeTx struct {
	pgx.Tx
	copied             map[string][][]any
	compositionInserts [][]any
}

func (tx *compositionScopeTx) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(query, "INSERT INTO army_compositions") {
		tx.compositionInserts = append(tx.compositionInserts, args)
	}
	return pgconn.NewCommandTag("INSERT 0 0"), nil
}
func (tx *compositionScopeTx) CopyFrom(_ context.Context, table pgx.Identifier, _ []string, source pgx.CopyFromSource) (int64, error) {
	for source.Next() {
		values, err := source.Values()
		if err != nil {
			return 0, err
		}
		tx.copied[table[0]] = append(tx.copied[table[0]], values)
	}
	return int64(len(tx.copied[table[0]])), source.Err()
}

type compositionScopeRow struct{}

func (compositionScopeRow) Scan(dest ...any) error { *dest[0].(*int) = 0; return nil }
func (*compositionScopeTx) QueryRow(context.Context, string, ...any) pgx.Row {
	return compositionScopeRow{}
}

func TestBattlelogWriterUpsertsShareCodeCompositionForRankedAndLegend(t *testing.T) {
	static, err := clashy.LoadStaticData()
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name             string
		modes            []string
		wantModes        []int16
		wantCompositions int
	}{
		{"ranked only", []string{"ranked"}, []int16{battleModeRanked}, 1},
		{"farming only", []string{"farming"}, []int16{battleModeFarming}, 0},
		{"legend", []string{"legend"}, []int16{battleModeLegend}, 1},
		{"mixed", []string{"ranked", "legend", "farming"}, []int16{battleModeRanked, battleModeLegend, battleModeFarming}, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const code = "u10x0"
			rows := make([]models.BattlelogRow, 0, len(tt.modes))
			for _, mode := range tt.modes {
				rows = append(rows, models.BattlelogRow{PlayerTag: "#P0Y", OpponentTag: "#P0L", BattleType: mode, ArmyShareCode: code, Timestamp: time.Now(), Attack: false})
			}
			tx := &compositionScopeTx{copied: make(map[string][][]any)}
			if _, err := (&timescaleBattlelogStore{static: static}).insertBattlelogRows(context.Background(), tx, rows); err != nil {
				t.Fatal(err)
			}
			if got := len(tx.compositionInserts); got != tt.wantCompositions {
				t.Fatalf("composition inserts=%d, want %d", got, tt.wantCompositions)
			}
			if tt.wantCompositions == 1 && tx.compositionInserts[0][0] != code {
				t.Fatalf("composition share code=%v, want %s", tx.compositionInserts[0][0], code)
			}
			staged := tx.copied["battlelog_ingest_stage"]
			if len(staged) != len(rows) {
				t.Fatalf("lost raw battles: %d", len(staged))
			}
			for i, row := range staged {
				if row[3] != tt.wantModes[i] || row[10] != code {
					t.Fatalf("raw mode/share code changed: %v", row)
				}
			}
		})
	}
}
