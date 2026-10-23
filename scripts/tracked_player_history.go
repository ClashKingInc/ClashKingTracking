package scripts

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"clashking_tracking/models"
	clashy "github.com/clashkinginc/clashy.go"
)

// These values and relative ID bases match the historical player change migration.
const (
	playerHistoryTroop         int16 = 1
	playerHistoryBoost         int16 = 2
	playerHistoryHero          int16 = 3
	playerHistorySpell         int16 = 4
	playerHistoryPet           int16 = 5
	playerHistoryEquipment     int16 = 6
	playerHistoryTownHall      int16 = 7
	playerHistoryBestTrophies  int16 = 8
	playerHistoryBestBuilder   int16 = 9
	playerHistoryExperience    int16 = 10
	playerHistoryWarPreference int16 = 11
	playerHistoryName          int16 = 12
)

type normalizedPlayerHistoryRow struct {
	EventTime     time.Time
	PlayerTag     string
	ChangeType    int16
	ItemID        *int16
	TownHallLevel *int16
	PreviousValue string
	CurrentValue  string
}

func normalizePlayerProfileChange(change models.PlayerProfileChangeRow, static *clashy.StaticData) []normalizedPlayerHistoryRow {
	if change.EventTime.IsZero() || strings.TrimSpace(change.PlayerTag) == "" {
		return nil
	}
	base := normalizedPlayerHistoryRow{EventTime: change.EventTime, PlayerTag: change.PlayerTag}
	if change.TownHallLevel > 0 && change.TownHallLevel <= math.MaxInt16 {
		value := int16(change.TownHallLevel)
		base.TownHallLevel = &value
	}
	if change.ChangeType == "name" {
		previous, previousOK := change.PreviousValue.(string)
		current, currentOK := change.CurrentValue.(string)
		if previousOK && currentOK && previous != "" && current != "" && previous != current {
			base.ChangeType, base.PreviousValue, base.CurrentValue = playerHistoryName, previous, current
			return []normalizedPlayerHistoryRow{base}
		}
		return nil
	}
	if change.ChangeType == "warPreference" {
		previous, previousOK := historyWarPreference(change.PreviousValue)
		current, currentOK := historyWarPreference(change.CurrentValue)
		if previousOK && currentOK && previous != current {
			base.ChangeType, base.PreviousValue, base.CurrentValue = playerHistoryWarPreference, previous, current
			return []normalizedPlayerHistoryRow{base}
		}
		return nil
	}
	var scalarType int16
	switch change.ChangeType {
	case "townHallLevel":
		scalarType = playerHistoryTownHall
	case "bestTrophies":
		scalarType = playerHistoryBestTrophies
	case "bestBuilderBaseTrophies":
		scalarType = playerHistoryBestBuilder
	case "expLevel":
		scalarType = playerHistoryExperience
	}
	if scalarType != 0 {
		previous, previousOK := historyNumber(change.PreviousValue)
		current, currentOK := historyNumber(change.CurrentValue)
		if previousOK && currentOK && current > previous {
			base.ChangeType = scalarType
			base.PreviousValue, base.CurrentValue = strconv.FormatInt(previous, 10), strconv.FormatInt(current, 10)
			return []normalizedPlayerHistoryRow{base}
		}
		return nil
	}
	section := change.ChangeType
	if section != "troops" && section != "heroes" && section != "spells" && section != "heroEquipment" {
		return nil
	}
	previous := historyItems(change.PreviousValue)
	current := historyItems(change.CurrentValue)
	rows := make([]normalizedPlayerHistoryRow, 0, len(current))
	for key, item := range current {
		name, _ := item["name"].(string)
		village, _ := item["village"].(string)
		old := previous[key]
		metadata, kind, baseID := historyStaticItem(static, section, name, village)
		if metadata == nil {
			continue
		}
		absoluteID, ok := historyNumber(metadata["_id"])
		if !ok || absoluteID < int64(baseID) || absoluteID-int64(baseID) > math.MaxInt16 {
			continue
		}
		id := int16(absoluteID - int64(baseID))
		row := base
		row.ItemID = &id
		if kind == playerHistoryBoost {
			if item["superTroopIsActive"] == true && old["superTroopIsActive"] != true {
				row.ChangeType, row.PreviousValue, row.CurrentValue = kind, "0", "1"
				rows = append(rows, row)
			}
			continue
		}
		oldLevel, oldOK := historyNumber(old["level"])
		newLevel, newOK := historyNumber(item["level"])
		if !newOK || newLevel <= 0 {
			continue
		}
		if !oldOK {
			oldLevel = 0
		}
		if newLevel <= oldLevel {
			continue
		}
		row.ChangeType = kind
		row.PreviousValue, row.CurrentValue = strconv.FormatInt(oldLevel, 10), strconv.FormatInt(newLevel, 10)
		rows = append(rows, row)
	}
	return rows
}

func historyItems(value any) map[string]map[string]any {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make(map[string]map[string]any, len(items))
	for _, value := range items {
		item, ok := value.(map[string]any)
		if !ok {
			continue
		}
		name, _ := item["name"].(string)
		village, _ := item["village"].(string)
		if name != "" {
			result[name+"/"+village] = item
		}
	}
	return result
}

func historyStaticItem(static *clashy.StaticData, section, name, village string) (map[string]any, int16, int) {
	if static == nil || name == "" {
		return nil, 0, 0
	}
	if section == "heroEquipment" {
		return static.LookupByName(name, "equipment", ""), playerHistoryEquipment, clashy.EquipmentBaseID
	}
	if section == "troops" {
		if pet := static.LookupByName(name, "pets", ""); pet != nil {
			return pet, playerHistoryPet, clashy.PetBaseID
		}
		if village == "" {
			village = "home"
		}
		troop := static.LookupByName(name, "troops", village)
		if troop != nil {
			if troop["super_troop"] != nil {
				return troop, playerHistoryBoost, clashy.TroopBaseID
			}
			return troop, playerHistoryTroop, clashy.TroopBaseID
		}
	}
	if section == "heroes" {
		if village == "" {
			village = "home"
		}
		return static.LookupByName(name, "heroes", village), playerHistoryHero, clashy.HeroBaseID
	}
	if section == "spells" {
		return static.LookupByName(name, "spells", ""), playerHistorySpell, clashy.SpellBaseID
	}
	return nil, 0, 0
}

func historyNumber(value any) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int64:
		return number, true
	case float64:
		if math.Trunc(number) == number && number >= math.MinInt64 && number < math.MaxInt64 {
			return int64(number), true
		}
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(number), 10, 64)
		return parsed, err == nil
	}
	return 0, false
}

func historyWarPreference(value any) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(fmt.Sprint(value))) {
	case "in", "true", "1":
		return "1", true
	case "out", "false", "0":
		return "0", true
	default:
		return "", false
	}
}
