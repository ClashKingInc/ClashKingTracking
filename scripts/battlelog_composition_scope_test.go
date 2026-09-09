//go:build script_internal_tests

package scripts

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"clashking_tracking/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type compositionScopeTx struct {
	pgx.Tx
	copied map[string][][]any
}

func (tx *compositionScopeTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
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

func TestBattlelogCompositionScope(t *testing.T) {
	for _, tt := range []struct {
		name  string
		modes []string
		want  int
	}{
		{"ranked only", []string{"ranked"}, 0},
		{"farming only", []string{"farming"}, 0},
		{"legend", []string{"legend"}, 1},
		{"mixed shared hash ranked first", []string{"ranked", "legend", "farming"}, 1},
		{"mixed shared hash legend first", []string{"legend", "ranked"}, 1},
		{"repeated legend", []string{"legend", "legend"}, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const code = "u10x0"
			hash := canonicalArmyHash(code)
			rows := make([]models.BattlelogRow, 0, len(tt.modes))
			for _, mode := range tt.modes {
				rows = append(rows, models.BattlelogRow{PlayerTag: "#P0Y", OpponentTag: "#P0L", BattleType: mode, ArmyShareCode: code, ArmyHash: hash, Timestamp: time.Now(), Attack: false})
			}
			tx := &compositionScopeTx{copied: make(map[string][][]any)}
			if _, err := (&timescaleBattlelogStore{}).insertBattlelogRows(context.Background(), tx, rows); err != nil {
				t.Fatal(err)
			}
			if got := len(tx.copied["battlelog_army_composition_stage"]); got != tt.want {
				t.Fatalf("composition rows=%d want=%d", got, tt.want)
			}
			staged := tx.copied["battlelog_ingest_stage"]
			if len(staged) != len(rows) {
				t.Fatalf("lost raw battles: %d", len(staged))
			}
			for i, row := range staged {
				if row[3] != tt.modes[i] || row[11] != code || !bytes.Equal(row[10].([]byte), hash[:]) {
					t.Fatalf("raw mode/share code/hash changed: %v", row)
				}
			}
		})
	}
}

func TestMixedBattlelogCompositionsExcludeRankedOnlyHash(t *testing.T) {
	tx := &compositionScopeTx{copied: make(map[string][][]any)}
	rows := []models.BattlelogRow{}
	for i, mode := range []string{"ranked", "legend"} {
		code := []string{"u10x0", "u11x0"}[i]
		rows = append(rows, models.BattlelogRow{PlayerTag: "#P0Y", OpponentTag: "#P0L", BattleType: mode, ArmyShareCode: code, ArmyHash: canonicalArmyHash(code), Timestamp: time.Now()})
	}
	if _, err := (&timescaleBattlelogStore{}).insertBattlelogRows(context.Background(), tx, rows); err != nil {
		t.Fatal(err)
	}
	staged := tx.copied["battlelog_army_composition_stage"]
	if len(staged) != 1 || !reflect.DeepEqual(staged[0][0], rows[1].ArmyHash[:]) {
		t.Fatal("expected only the Legend composition")
	}
}
