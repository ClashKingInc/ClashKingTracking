package models

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

func ComputeWarID(clanTag, opponentTag string, prep time.Time) string {
	// Sort the tags first so either clan can poll the same war and still resolve
	// to one stable storage key.
	tags := []string{clanTag, opponentTag}
	sort.Strings(tags)
	return strings.Join(tags, "-") + "-" + fmt.Sprintf("%d", prep.UTC().Unix())
}
