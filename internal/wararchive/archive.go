package wararchive

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Dictionary is shared by the migration, live archiver, and API reader.
//
//go:embed war-json.zdict
var Dictionary []byte

const (
	BattleModifierNone       = "none"
	BattleModifierHardMode   = "hardMode"
	BattleModifierMinusOne   = "minusOne"
	BattleModifierMinusTwo   = "minusTwo"
	BattleModifierMinusThree = "minusThree"
)

// NormalizeBattleModifier maps legacy missing/null values and spelling variants
// to the canonical Clash API enum stored in SQL and R2.
func NormalizeBattleModifier(value string) string {
	key := strings.ToLower(strings.TrimSpace(value))
	key = strings.NewReplacer("_", "", "-", "", " ", "").Replace(key)
	switch key {
	case "", "null", BattleModifierNone:
		return BattleModifierNone
	case "hardmode":
		return BattleModifierHardMode
	case "minusone":
		return BattleModifierMinusOne
	case "minustwo":
		return BattleModifierMinusTwo
	case "minusthree":
		return BattleModifierMinusThree
	default:
		return BattleModifierNone
	}
}

// War is the canonical R2 frame. The SQL integer ID deliberately stays outside it.
type War struct {
	ID                   int32     `json:"-"`
	WarTag               string    `json:"warTag,omitempty"`
	State                string    `json:"state"`
	TeamSize             int       `json:"teamSize"`
	AttacksPerMember     int       `json:"attacksPerMember"`
	PreparationStartTime time.Time `json:"preparationStartTime"`
	StartTime            time.Time `json:"startTime"`
	EndTime              time.Time `json:"endTime"`
	BattleModifier       string    `json:"battleModifier"`
	Clan                 Clan      `json:"clan"`
	Opponent             Clan      `json:"opponent"`
}

type Clan struct {
	Tag                   string   `json:"tag"`
	Name                  string   `json:"name"`
	BadgeToken            string   `json:"badgeToken"`
	ClanLevel             int      `json:"clanLevel"`
	Attacks               int      `json:"attacks"`
	Stars                 int      `json:"stars"`
	DestructionPercentage float64  `json:"destructionPercentage"`
	Members               []Member `json:"members"`
}

type Member struct {
	Tag           string   `json:"tag"`
	Name          string   `json:"name"`
	TownhallLevel int      `json:"townhallLevel"`
	MapPosition   int      `json:"mapPosition"`
	Attacks       []Attack `json:"attacks"`
}

// AttackerTag is reconstructed from the containing member.
type Attack struct {
	DefenderTag           string `json:"defenderTag"`
	Stars                 int    `json:"stars"`
	DestructionPercentage int    `json:"destructionPercentage"`
	Duration              int    `json:"duration"`
	Order                 int    `json:"order"`
}

type Locator struct {
	WarID           int32
	PackID          int64
	Offset          int64
	CompressedBytes int
	RawBytes        int
}

type PackStats struct {
	ByDay map[string]DayStats `json:"byDay"`
	CWL   *CWLStats           `json:"cwl,omitempty"`
}

type DayStats struct {
	WarsByType         map[string]int                 `json:"warsByType"`
	TotalAttacks       int                            `json:"totalAttacks"`
	TotalMissedAttacks int                            `json:"totalMissedAttacks"`
	WarsBySize         map[string]int                 `json:"warsBySize"`
	RegularHitRates    map[string]HitRateStats        `json:"regularHitRates"`
	RegularByWarSize   map[string]RegularWarSizeStats `json:"regularByWarSize"`
}

type HitRateStats struct {
	Attacks    int                   `json:"attacks"`
	ZeroStars  StarOutcomeStats      `json:"zeroStars"`
	OneStars   StarOutcomeStats      `json:"oneStars"`
	TwoStars   StarOutcomeStats      `json:"twoStars"`
	ThreeStars ThreeStarOutcomeStats `json:"threeStars"`
}

type StarOutcomeStats struct {
	Attacks            int   `json:"attacks"`
	DestructionPercent int64 `json:"destructionPercent"`
	DurationSeconds    int64 `json:"durationSeconds"`
}

type ThreeStarOutcomeStats struct {
	Attacks         int   `json:"attacks"`
	DurationSeconds int64 `json:"durationSeconds"`
}

type RegularWarSizeStats struct {
	Wars       int            `json:"wars"`
	Townhalls  map[string]int `json:"townhalls"`
	TotalStars int            `json:"totalStars"`
	Wins       int            `json:"wins"`
	Losses     int            `json:"losses"`
	Ties       int            `json:"ties"`
}

func Marshal(war War) ([]byte, error) {
	if war.StartTime.IsZero() {
		return nil, fmt.Errorf("archive war is missing startTime")
	}
	return json.Marshal(war)
}

func Unmarshal(data []byte) (War, error) {
	var war War
	err := json.Unmarshal(data, &war)
	return war, err
}

type PackBuilder struct {
	id      int64
	buffer  bytes.Buffer
	encoder *zstd.Encoder
	rows    []Locator
}

func NewPackBuilder(id int64) (*PackBuilder, error) {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(true),
		zstd.WithEncoderDict(Dictionary),
	)
	if err != nil {
		return nil, err
	}
	return &PackBuilder{id: id, encoder: encoder}, nil
}

func (b *PackBuilder) Add(warID int32, war War) (Locator, error) {
	raw, err := Marshal(war)
	if err != nil {
		return Locator{}, err
	}
	compressed := b.encoder.EncodeAll(raw, nil)
	locator := Locator{WarID: warID, PackID: b.id, Offset: int64(b.buffer.Len()), CompressedBytes: len(compressed), RawBytes: len(raw)}
	_, _ = b.buffer.Write(compressed)
	b.rows = append(b.rows, locator)
	return locator, nil
}

func (b *PackBuilder) Bytes() []byte       { return b.buffer.Bytes() }
func (b *PackBuilder) Locators() []Locator { return b.rows }
func (b *PackBuilder) Close()              { b.encoder.Close() }

func ObjectKey(packID int64) string { return fmt.Sprintf("packs/%06d.pack", packID) }

func NewPackStats() PackStats {
	return PackStats{ByDay: map[string]DayStats{}}
}

func newDayStats() DayStats {
	return DayStats{
		WarsByType:       map[string]int{},
		WarsBySize:       map[string]int{},
		RegularHitRates:  map[string]HitRateStats{},
		RegularByWarSize: map[string]RegularWarSizeStats{},
	}
}

func (s *PackStats) Add(warType string, war War) {
	warType = dimension(warType, "random")
	dayKey := war.EndTime.UTC().Format("2006-01-02")
	day, exists := s.ByDay[dayKey]
	if !exists {
		day = newDayStats()
	}
	size := fmt.Sprint(war.TeamSize)
	day.WarsByType[warType]++
	day.WarsBySize[size]++
	clanAttacks := countAttacks(war.Clan)
	opponentAttacks := countAttacks(war.Opponent)
	day.TotalAttacks += clanAttacks + opponentAttacks
	day.TotalMissedAttacks += missedAttacks(war, clanAttacks) + missedAttacks(war, opponentAttacks)
	if warType == "random" {
		addRegularWarSize(&day, size, war)
		addRegularHitRates(&day, war.Clan, war.Opponent)
		addRegularHitRates(&day, war.Opponent, war.Clan)
	}
	s.ByDay[dayKey] = day
}

func (s PackStats) TotalAttacks() int {
	total := 0
	for _, day := range s.ByDay {
		total += day.TotalAttacks
	}
	return total
}

func countAttacks(clan Clan) int {
	total := 0
	for _, member := range clan.Members {
		total += len(member.Attacks)
	}
	return total
}

func missedAttacks(war War, used int) int {
	size := war.TeamSize
	if size <= 0 {
		size = max(len(war.Clan.Members), len(war.Opponent.Members))
	}
	attacksPerMember := war.AttacksPerMember
	if attacksPerMember <= 0 {
		attacksPerMember = 1
	}
	return max(0, size*attacksPerMember-used)
}

func addRegularWarSize(day *DayStats, size string, war War) {
	value, exists := day.RegularByWarSize[size]
	if !exists {
		value.Townhalls = map[string]int{}
	}
	value.Wars++
	value.TotalStars += war.Clan.Stars + war.Opponent.Stars
	for _, clan := range []Clan{war.Clan, war.Opponent} {
		for _, member := range clan.Members {
			value.Townhalls[fmt.Sprint(member.TownhallLevel)]++
		}
	}
	if war.Clan.Stars == war.Opponent.Stars && war.Clan.DestructionPercentage == war.Opponent.DestructionPercentage {
		value.Ties += 2
	} else {
		value.Wins++
		value.Losses++
	}
	day.RegularByWarSize[size] = value
}

func addRegularHitRates(day *DayStats, attacking, defending Clan) {
	defenders := make(map[string]int, len(defending.Members))
	for _, member := range defending.Members {
		defenders[member.Tag] = member.TownhallLevel
	}
	for _, member := range attacking.Members {
		for _, attack := range member.Attacks {
			defenderTH := defenders[attack.DefenderTag]
			key := fmt.Sprintf("%d:%d", member.TownhallLevel, defenderTH)
			value := day.RegularHitRates[key]
			value.Attacks++
			switch attack.Stars {
			case 3:
				value.ThreeStars.Attacks++
				value.ThreeStars.DurationSeconds += int64(attack.Duration)
			case 2:
				addStarOutcome(&value.TwoStars, attack)
			case 1:
				addStarOutcome(&value.OneStars, attack)
			default:
				addStarOutcome(&value.ZeroStars, attack)
			}
			day.RegularHitRates[key] = value
		}
	}
}

func addStarOutcome(value *StarOutcomeStats, attack Attack) {
	value.Attacks++
	value.DestructionPercent += int64(attack.DestructionPercentage)
	value.DurationSeconds += int64(attack.Duration)
}

func dimension(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.ToLower(value)
}
