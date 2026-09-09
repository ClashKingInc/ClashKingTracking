package models

type PlayerProfileIngest struct {
	Player       BasicPlayerRow
	Heroes       []PlayerHeroRow
	Equipment    []PlayerEquipmentRow
	Achievements []PlayerAchievementRow
}

type PlayerHeroRow struct {
	Name     string `json:"name"`
	Level    int    `json:"level"`
	MaxLevel int    `json:"max_level"`
	Village  string `json:"village"`
}

type PlayerEquipmentRow struct {
	Name     string `json:"name"`
	Level    int    `json:"level"`
	MaxLevel int    `json:"max_level"`
	Village  string `json:"village"`
	Rarity   string `json:"rarity,omitempty"`
}

type PlayerAchievementRow struct {
	Name    string `json:"name"`
	Stars   int    `json:"stars"`
	Value   int    `json:"value"`
	Target  int    `json:"target"`
	Village string `json:"village"`
}
