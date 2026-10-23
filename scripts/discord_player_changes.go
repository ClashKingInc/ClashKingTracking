package scripts

import (
	"encoding/json"

	"clashking_tracking/models"
	clashy "github.com/clashkinginc/clashy.go"
)

// The troop array contains both permanent levels and temporary super boosts.
// A changed array alone is not evidence of either particular event.
func playerLogTypesForProfileChange(change models.PlayerProfileChangeRow) []string {
	if change.ChangeType == "clanCapitalContributions" && intValue(change.CurrentValue) <= intValue(change.PreviousValue) {
		return nil
	}
	if change.ChangeType != "troops" {
		return playerLogTypesForChange(change.ChangeType)
	}
	previous := discordTroops(change.PreviousValue)
	current := discordTroops(change.CurrentValue)
	levels, boosts := false, false
	for key, troop := range current {
		old := previous[key]
		isSuper := (clashy.Troop{Name: stringMapValue(troop, "name")}).IsSuperTroop()
		if intValue(troop["level"]) > intValue(old["level"]) && !isSuper {
			levels = true
		}
		if isSuper && troop["superTroopIsActive"] == true && old["superTroopIsActive"] != true {
			boosts = true
		}
	}
	var types []string
	if levels {
		types = append(types, "troop_upgrade")
	}
	if boosts {
		types = append(types, "super_troop_boost")
	}
	return types
}

func discordTroops(value any) map[string]map[string]any {
	var troops []map[string]any
	raw, err := json.Marshal(value)
	if err != nil || json.Unmarshal(raw, &troops) != nil {
		return nil
	}
	result := make(map[string]map[string]any, len(troops))
	for _, troop := range troops {
		result[stringMapValue(troop, "name")+"/"+stringMapValue(troop, "village")] = troop
	}
	return result
}
