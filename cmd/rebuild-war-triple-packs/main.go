// Read-only archive replay into disposable, bounded SQL staging. The command
// never opens a production SQL session or changes a leaderboard table.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"clashking_tracking/internal/wararchive"
	"clashking_tracking/scripts"
	"github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
	"github.com/klauspost/compress/zstd"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type replayStats struct {
	AsOf                    time.Time  `json:"as_of"`
	Cutoff                  time.Time  `json:"cutoff"`
	PhysicalRows            int64      `json:"physical_rows"`
	CanonicalWars           int64      `json:"canonical_wars"`
	DuplicateRows           int64      `json:"duplicate_rows"`
	Excluded                int64      `json:"excluded"`
	Unscorable              int64      `json:"unscorable"`
	Packs                   int64      `json:"packs"`
	DownloadBytes           int64      `json:"download_bytes"`
	PlayerKeys              int64      `json:"player_keys"`
	QualifiedSuffixRows     int64      `json:"qualified_suffix_rows"`
	ExactKeys               int64      `json:"exact_keys"`
	UnavailableKeys         int64      `json:"unavailable_keys"`
	GlobalBarrier           *time.Time `json:"global_barrier"`
	ManifestScorable        bool       `json:"manifest_scorable"`
	SourceCoverageValidated bool       `json:"source_coverage_validated"`
	Complete                bool       `json:"complete"`
}

type stagedWar struct {
	id                    int32
	end                   time.Time
	identity, fingerprint string
	known                 bool
	reason                string
}
type stagedOutcome struct {
	war       stagedWar
	member    wararchive.Member
	clan      string
	qualified *bool
	credits   int
}

func run() error {
	index := flag.String("index-file", "", "reviewed seven-column CSV manifest, ordered by pack ID; no automatic production export")
	output := flag.String("output-dir", "", "new directory for coverage.json and preview.csv")
	asOfText := flag.String("as-of", "", "required frozen UTC RFC3339 window end")
	dsn := flag.String("staging-dsn", "postgres://clashking_local:clashking_local@127.0.0.1:54330/clashking_dev", "disposable loopback staging database only")
	archiveBase := flag.String("archive-base", "https://wars.clashk.ing/", "archive origin (loopback permitted for actual service E2E)")
	packLimit := flag.Int("pack-limit", 0, "bounded diagnostic limit; always marks coverage incomplete")
	apply := flag.Bool("apply", false, "unsupported: production application requires a separately reviewed artifact")
	flag.Parse()
	if *apply {
		return fmt.Errorf("score application is disabled; review the coverage preview before preparing an apply artifact")
	}
	asOf, err := time.Parse(time.RFC3339Nano, *asOfText)
	if err != nil {
		return fmt.Errorf("explicit RFC3339 as-of is required")
	}
	asOf = asOf.UTC()
	var cutoff time.Time
	if *index == "" || *output == "" || *packLimit < 0 {
		return fmt.Errorf("index-file and new output-dir are required")
	}
	u, err := url.Parse(*dsn)
	canonical := err == nil && u.Hostname() == "127.0.0.1" && u.Port() == "54330" && u.Path == "/clashking_dev"
	disposable := err == nil && os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && *dsn == os.Getenv("TEST_DATABASE_URL") && u.Hostname() == "127.0.0.1" && u.Path == "/clashking_test"
	if !canonical && !disposable {
		return fmt.Errorf("staging is restricted to the canonical loopback development database")
	}
	base, err := url.Parse(*archiveBase)
	if err != nil || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return fmt.Errorf("invalid archive origin")
	}
	if !(base.Scheme == "https" && base.Host == "wars.clashk.ing") && !(base.Scheme == "http" && (base.Hostname() == "127.0.0.1" || base.Hostname() == "localhost")) {
		return fmt.Errorf("archive origin must be the canonical public archive or loopback")
	}
	if err = os.Mkdir(*output, 0700); err != nil {
		return err
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	// PostgreSQL defines the production calendar month, including month-end
	// clamping. Go AddDate normalizes Feb29 differently and must not set it.
	if err = conn.QueryRow(ctx, `SELECT (($1::timestamptz AT TIME ZONE 'UTC') - interval '1 month') AT TIME ZONE 'UTC'`, asOf).Scan(&cutoff); err != nil {
		return err
	}
	cutoff = cutoff.UTC()
	if _, err = conn.Exec(ctx, stagingSchema); err != nil {
		return err
	}
	f, err := os.Open(*index)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderDicts(wararchive.Dictionary), zstd.WithDecoderMaxMemory(32<<20))
	if err != nil {
		return err
	}
	defer decoder.Close()
	stats := replayStats{AsOf: asOf, Cutoff: cutoff, ManifestScorable: *packLimit == 0}
	reader := csv.NewReader(f)
	reader.FieldsPerRecord = 7
	client := &http.Client{Timeout: 90 * time.Second}
	var currentPack int64 = -1
	var data []byte
	var batch []stagedWar
	var outcomes []stagedOutcome
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := stage(ctx, conn, batch, outcomes, &stats); err != nil {
			return err
		}
		batch = nil
		outcomes = nil
		return nil
	}
	for {
		row, e := reader.Read()
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		nums := make([]int64, 5)
		for i := range nums {
			nums[i], e = strconv.ParseInt(row[i], 10, 64)
			if e != nil {
				return fmt.Errorf("invalid manifest integer column %d", i+1)
			}
		}
		if nums[0] < 0 || nums[1] <= 0 || nums[1] > 2147483647 {
			return fmt.Errorf("invalid source-assigned IDs")
		}
		if nums[0] != currentPack {
			if err = flush(); err != nil {
				return err
			}
			if nums[0] < currentPack {
				return fmt.Errorf("manifest must be ordered by pack ID, then frame offset")
			}
			if *packLimit > 0 && stats.Packs >= int64(*packLimit) {
				stats.ManifestScorable = false
				break
			}
			currentPack = nums[0]
			data = nil
			if currentPack > 0 {
				archiveURL := *base
				archiveURL.Path = strings.TrimRight(base.Path, "/") + "/" + wararchive.ObjectKey(currentPack)
				req, _ := http.NewRequestWithContext(ctx, "GET", archiveURL.String(), nil)
				resp, e := client.Do(req)
				if e != nil {
					return e
				}
				data, e = io.ReadAll(io.LimitReader(resp.Body, 128<<20))
				resp.Body.Close()
				if e != nil {
					return e
				}
				if resp.StatusCode != 200 || len(data) == 0 || len(data) >= 128<<20 {
					return fmt.Errorf("pack %d invalid response: status=%d bytes=%d", currentPack, resp.StatusCode, len(data))
				}
				stats.Packs++
				stats.DownloadBytes += int64(len(data))
			}
		}
		stats.PhysicalRows++
		var raw []byte
		if currentPack == 0 {
			raw, e = base64.StdEncoding.DecodeString(strings.ReplaceAll(row[5], "\n", ""))
		} else {
			offset, size := nums[2], nums[3]
			if offset < 0 || size <= 0 || size > 2<<20 || offset > int64(len(data))-size {
				return fmt.Errorf("war %d invalid pack locator", nums[1])
			}
			raw, e = decoder.DecodeAll(data[offset:offset+size], nil)
		}
		if e != nil {
			return e
		}
		if len(raw) == 0 {
			return fmt.Errorf("war %d missing source payload; coverage cannot be published", nums[1])
		}
		war, e := wararchive.Unmarshal(raw)
		if e != nil {
			return e
		}
		end := time.Unix(nums[4], 0).UTC()
		if !war.EndTime.Equal(end) {
			return fmt.Errorf("war %d frame end does not match index", nums[1])
		}
		if end.Before(cutoff) || end.After(asOf) {
			stats.Excluded++
			continue
		}
		warType := row[6]
		if warType != "random" || !strings.EqualFold(war.State, "warEnded") || clashy.IsFriendlyWarWindow(war.PreparationStartTime, war.StartTime, war.EndTime) {
			stats.Excluded++
			continue
		}
		if war.Clan.Tag == "" || war.Opponent.Tag == "" || war.Clan.Tag == war.Opponent.Tag || war.PreparationStartTime.IsZero() {
			return fmt.Errorf("war %d lacks canonical identity", nums[1])
		}
		normalized := war
		if normalized.Clan.Tag > normalized.Opponent.Tag {
			normalized.Clan, normalized.Opponent = normalized.Opponent, normalized.Clan
		}
		identity := fmt.Sprintf("%s|%s|%d|%d", normalized.Clan.Tag, normalized.Opponent.Tag, normalized.PreparationStartTime.Unix(), end.Unix())
		fingerprint, e := scoringFingerprint(normalized)
		if e != nil {
			return e
		}
		entry := stagedWar{id: int32(nums[1]), end: end, identity: identity, fingerprint: fingerprint, known: true}
		// A complete roster still identifies affected keys when the archive
		// omits its allowance. Validate its members separately, but never
		// award credits or manufacture resets from that unknown source.
		rosterWar := war
		if rosterWar.AttacksPerMember != 2 {
			rosterWar.AttacksPerMember = 2
		}
		scored, e := scripts.ScoreWarTripleStreaks(rosterWar, "random")
		if e != nil || len(scored) == 0 || war.AttacksPerMember != 2 {
			entry.known = false
			entry.reason = "incomplete_or_invalid_roster"
			if war.AttacksPerMember != 2 {
				entry.reason = "unknown_attack_allowance"
			}
			stats.Unscorable++
			// A complete roster identifies affected keys even when attack rules are unknown.
			// A missing/invalid roster cannot identify everyone; retain a global barrier.
			if len(scored) == 0 || e != nil {
				if stats.GlobalBarrier == nil || end.After(*stats.GlobalBarrier) {
					v := end
					stats.GlobalBarrier = &v
				}
			}
		}
		batch = append(batch, entry)
		for _, o := range scored {
			var q *bool
			if entry.known {
				v := o.Qualified
				q = &v
			}
			outcomes = append(outcomes, stagedOutcome{entry, o.Member, o.Clan, q, o.CreditedTriples})
		}
		if len(batch) >= 100 {
			if err = flush(); err != nil {
				return err
			}
		}
	}
	if err = flush(); err != nil {
		return err
	}
	if err = finish(ctx, conn, *output, &stats); err != nil {
		return err
	}
	encoded, _ := json.MarshalIndent(stats, "", "  ")
	if err = os.WriteFile(filepath.Join(*output, "coverage.json"), append(encoded, '\n'), 0600); err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}

// Hash only scoring semantics. Clan/player names and other mutable display
// fields cannot turn repeated observations of the same game war into conflicts.
func scoringFingerprint(war wararchive.War) (string, error) {
	for _, side := range []*wararchive.Clan{&war.Clan, &war.Opponent} {
		side.Members = append([]wararchive.Member(nil), side.Members...)
		side.Name = ""
		side.BadgeToken = ""
		side.ClanLevel = 0
		side.Attacks = 0
		side.Stars = 0
		side.DestructionPercentage = 0
		sort.Slice(side.Members, func(i, j int) bool { return side.Members[i].Tag < side.Members[j].Tag })
		for i := range side.Members {
			m := &side.Members[i]
			m.Attacks = append([]wararchive.Attack(nil), m.Attacks...)
			m.Name = ""
			m.MapPosition = 0
			sort.Slice(m.Attacks, func(i, j int) bool { return m.Attacks[i].Order < m.Attacks[j].Order })
			for j := range m.Attacks {
				m.Attacks[j].DestructionPercentage = 0
				m.Attacks[j].Duration = 0
			}
		}
	}
	raw, err := json.Marshal(war)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
