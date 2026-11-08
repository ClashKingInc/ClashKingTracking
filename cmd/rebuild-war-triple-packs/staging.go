package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/jackc/pgx/v5"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// This one-month operational replay keeps only each player's boundaries in
// memory. Qualified events spill to disk and are streamed once after all reset
// boundaries are known; source pack order must never determine the result.
const stagingSchema = `SELECT 1`

type playerKey struct {
	tag string
	th  int
}
type event struct {
	end        int64
	identity   string
	warID      int32
	name, clan string
	credits    int
}
type playerState struct {
	latest         event
	reset, unknown *event
	score          int
}
type acceptedWar struct {
	fingerprint string
	id          int32
}

var replay = struct {
	wars    map[string]acceptedWar
	players map[playerKey]*playerState
	spool   *os.File
	writer  *csv.Writer
}{wars: map[string]acceptedWar{}, players: map[playerKey]*playerState{}}

func newer(a, b event) bool { return a.end > b.end || a.end == b.end && a.identity > b.identity }
func eventRow(key playerKey, e event) []string {
	return []string{key.tag, strconv.Itoa(key.th), strconv.FormatInt(int64(e.warID), 10), time.Unix(e.end, 0).UTC().Format(time.RFC3339), e.identity, e.name, e.clan, strconv.Itoa(e.credits)}
}
func stage(ctx context.Context, _ *pgx.Conn, wars []stagedWar, outcomes []stagedOutcome, stats *replayStats) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if replay.spool == nil {
		f, e := os.CreateTemp("", "ck-war-qualified-*.csv")
		if e != nil {
			return e
		}
		replay.spool = f
		replay.writer = csv.NewWriter(f)
	}
	accepted := map[string]int32{}
	for _, w := range wars {
		if prior, ok := replay.wars[w.identity]; ok {
			if prior.fingerprint != w.fingerprint {
				return fmt.Errorf("canonical source versions disagree on roster or scoring; preview withheld")
			}
			stats.DuplicateRows++
			continue
		}
		replay.wars[w.identity] = acceptedWar{w.fingerprint, w.id}
		accepted[w.identity] = w.id
		stats.CanonicalWars++
	}
	seen := map[struct {
		key      playerKey
		identity string
	}]bool{}
	for _, o := range outcomes {
		id, ok := accepted[o.war.identity]
		if !ok || id != o.war.id {
			continue
		}
		key := playerKey{o.member.Tag, o.member.TownhallLevel}
		unique := struct {
			key      playerKey
			identity string
		}{key, o.war.identity}
		if seen[unique] {
			continue
		}
		seen[unique] = true
		item := event{o.war.end.Unix(), o.war.identity, o.war.id, o.member.Name, o.clan, o.credits}
		state := replay.players[key]
		if state == nil {
			if len(replay.players) >= 20000000 {
				return fmt.Errorf("replay exceeds reviewed 20-million-key memory bound")
			}
			state = &playerState{latest: item}
			replay.players[key] = state
		} else if newer(item, state.latest) {
			state.latest = item
		}
		if o.qualified == nil {
			if state.unknown == nil || newer(item, *state.unknown) {
				copy := item
				state.unknown = &copy
			}
			continue
		}
		if !*o.qualified {
			if state.reset == nil || newer(item, *state.reset) {
				copy := item
				state.reset = &copy
			}
			continue
		}
		if e := replay.writer.Write(eventRow(key, item)); e != nil {
			return e
		}
	}
	replay.writer.Flush()
	return replay.writer.Error()
}
func csvFile(output, name string, header []string) (*os.File, *csv.Writer, error) {
	f, e := os.OpenFile(filepath.Join(output, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return nil, nil, e
	}
	w := csv.NewWriter(f)
	e = w.Write(header)
	if e != nil {
		f.Close()
		return nil, nil, e
	}
	return f, w, nil
}
func closeCSV(f *os.File, w *csv.Writer) error {
	w.Flush()
	if e := w.Error(); e != nil {
		f.Close()
		return e
	}
	if e := f.Sync(); e != nil {
		f.Close()
		return e
	}
	return f.Close()
}
func available(s *playerState, stats *replayStats) bool {
	if s.unknown != nil && (s.reset == nil || !newer(*s.reset, *s.unknown)) {
		return false
	}
	if stats.GlobalBarrier != nil && (s.reset == nil || s.reset.end <= stats.GlobalBarrier.Unix()) {
		return false
	}
	return true
}
func finish(ctx context.Context, _ *pgx.Conn, output string, stats *replayStats) error {
	header := []string{"player_tag", "townhall_level", "war_id", "war_end_time", "identity", "player_name", "clan_tag", "credited_triples"}
	f, w, e := csvFile(output, "qualified-suffix.csv", header)
	if e != nil {
		return e
	}
	if replay.spool != nil {
		defer os.Remove(replay.spool.Name())
		defer replay.spool.Close()
		if e = replay.spool.Sync(); e != nil {
			return e
		}
		if _, e = replay.spool.Seek(0, 0); e != nil {
			return e
		}
		r := csv.NewReader(replay.spool)
		for {
			if e = ctx.Err(); e != nil {
				return e
			}
			row, e := r.Read()
			if e == io.EOF {
				break
			}
			if e != nil {
				return e
			}
			th, e := strconv.Atoi(row[1])
			if e != nil {
				return e
			}
			end, e := time.Parse(time.RFC3339, row[3])
			if e != nil {
				return e
			}
			key := playerKey{row[0], th}
			state := replay.players[key]
			if state == nil {
				return fmt.Errorf("qualified event has no player state")
			}
			item := event{end: end.Unix(), identity: row[4]}
			if state.reset != nil && !newer(item, *state.reset) {
				continue
			}
			credits, e := strconv.Atoi(row[7])
			if e != nil {
				return e
			}
			state.score += credits
			stats.QualifiedSuffixRows++
			if e = w.Write(row); e != nil {
				return e
			}
		}
	}
	if e = closeCSV(f, w); e != nil {
		return e
	}
	keys := make([]playerKey, 0, len(replay.players))
	for k := range replay.players {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].tag == keys[j].tag {
			return keys[i].th < keys[j].th
		}
		return keys[i].tag < keys[j].tag
	})
	preview, pw, e := csvFile(output, "preview.csv", []string{"player_tag", "townhall_level", "streak", "available", "latest_end", "player_name", "clan_tag", "reset_end", "unknown_end"})
	if e != nil {
		return e
	}
	resets, rw, e := csvFile(output, "latest-resets.csv", header[:7])
	if e != nil {
		return e
	}
	barriers, bw, e := csvFile(output, "coverage-barriers.csv", []string{"player_tag", "townhall_level", "unknown_end", "unknown_identity"})
	if e != nil {
		return e
	}
	for _, key := range keys {
		s := replay.players[key]
		ok := available(s, stats)
		score, resetEnd, unknownEnd := "", "", ""
		stats.PlayerKeys++
		if ok {
			stats.ExactKeys++
			score = strconv.Itoa(s.score)
		} else {
			stats.UnavailableKeys++
		}
		if s.reset != nil {
			resetEnd = time.Unix(s.reset.end, 0).UTC().Format(time.RFC3339)
			if e = rw.Write(eventRow(key, *s.reset)[:7]); e != nil {
				return e
			}
		}
		if s.unknown != nil {
			unknownEnd = time.Unix(s.unknown.end, 0).UTC().Format(time.RFC3339)
			if e = bw.Write([]string{key.tag, strconv.Itoa(key.th), unknownEnd, s.unknown.identity}); e != nil {
				return e
			}
		}
		boolText := "f"
		if ok {
			boolText = "t"
		}
		if e = pw.Write([]string{key.tag, strconv.Itoa(key.th), score, boolText, time.Unix(s.latest.end, 0).UTC().Format(time.RFC3339), s.latest.name, s.latest.clan, resetEnd, unknownEnd}); e != nil {
			return e
		}
	}
	for _, x := range []struct {
		f *os.File
		w *csv.Writer
	}{{preview, pw}, {resets, rw}, {barriers, bw}} {
		if e = closeCSV(x.f, x.w); e != nil {
			return e
		}
	}
	if stats.UnavailableKeys > 0 || stats.GlobalBarrier != nil {
		stats.ManifestScorable = false
	}
	fmt.Printf("preview physical=%d canonical=%d keys=%d exact=%d unavailable=%d\n", stats.PhysicalRows, stats.CanonicalWars, stats.PlayerKeys, stats.ExactKeys, stats.UnavailableKeys)
	return nil
}
