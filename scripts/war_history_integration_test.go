//go:build script_internal_tests

package scripts

import (
	"context"
	"os"
	"testing"
	"time"

	"clashking_tracking/models"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestFinishedNormalAndCWLWarsStoreParticipantHistoryAtomically(t *testing.T) {
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" {
		t.Skip("requires disposable Goose fixture")
	}
	pool, err := pgxpool.New(t.Context(), os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := &timescaleWarStore{pool: pool}

	for _, test := range []struct {
		name              string
		warTag            string
		warID             int32
		clanTag           string
		opponentTag       string
		clanPlayerTag     string
		opponentPlayerTag string
	}{
		{name: "normal", warID: 900_000, clanTag: "#P0289", opponentTag: "#Y0289", clanPlayerTag: "#Q0289", opponentPlayerTag: "#G0289"},
		{name: "cwl", warTag: "#P0Y", warID: 900_001, clanTag: "#R0289", opponentTag: "#J0289", clanPlayerTag: "#C0289", opponentPlayerTag: "#U0289"},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
			war := sampleWar(now.Add(-48*time.Hour), now.Add(-24*time.Hour), now)
			war.Clan.Tag = test.clanTag
			war.Opponent.Tag = test.opponentTag
			war.Clan.Members[0].Tag = test.clanPlayerTag
			war.Opponent.Members[0].Tag = test.opponentPlayerTag

			ingest, err := buildWarIngest(war, war.Clan.Tag, true, test.warTag, "", test.warID)
			if err != nil {
				t.Fatal(err)
			}
			// This test stores an already-finalized fixture directly; production
			// scheduled finalization supplies a schedule key and rewrites the ID.
			ingest.FinishedScheduleKey = ""
			if err := store.Store(t.Context(), ingest); err != nil {
				t.Fatal(err)
			}

			for _, tag := range ingest.ArchiveParticipants {
				var contains bool
				if err := pool.QueryRow(t.Context(), `
					SELECT $2::integer = ANY(war_ids)
					FROM player_war_history
					WHERE player_tag = $1
				`, tag, ingest.FinishedWarID).Scan(&contains); err != nil {
					t.Fatalf("load history for %s: %v", tag, err)
				}
				if !contains {
					t.Fatalf("history for %s does not contain war %d", tag, ingest.FinishedWarID)
				}
			}
		})
	}
}

func TestFinishedWarHistoryRollsBackWithLaterStoreFailure(t *testing.T) {
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" {
		t.Skip("requires disposable Goose fixture")
	}
	pool, err := pgxpool.New(t.Context(), os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := &timescaleWarStore{pool: pool}

	now := time.Date(2026, 9, 11, 13, 0, 0, 0, time.UTC)
	war := sampleWar(now.Add(-48*time.Hour), now.Add(-24*time.Hour), now)
	war.Clan.Tag = "#V0289"
	war.Opponent.Tag = "#L0289"
	war.Clan.Members[0].Tag = "#P0R89"
	war.Opponent.Members[0].Tag = "#Y0J89"
	ingest, err := buildWarIngest(war, war.Clan.Tag, true, "", "", 900_002)
	if err != nil {
		t.Fatal(err)
	}
	ingest.FinishedScheduleKey = ""
	// The malformed group fails after player history is written, proving the
	// history update is covered by the enclosing finished-war transaction.
	ingest.CWLGroups = []models.CWLGroupRow{{CWLID: "ROLLBACKBAD1", Season: "2026-09", State: "invalid"}}
	if err := store.Store(t.Context(), ingest); err == nil {
		t.Fatal("expected malformed CWL group to fail the finished-war transaction")
	}
	for _, tag := range ingest.ArchiveParticipants {
		var count int
		if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM player_war_history WHERE player_tag=$1`, tag).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("rolled-back history remains for %s", tag)
		}
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM wars WHERE war_id=$1`, ingest.FinishedWarID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("rolled-back war index remains for %d", ingest.FinishedWarID)
	}
}
