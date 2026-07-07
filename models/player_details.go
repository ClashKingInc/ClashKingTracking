package models

type PlayerProfileIngest struct {
	Player    BasicPlayerRow
	Troops    []PlayerTroopRow
	Spells    []PlayerSpellRow
	Heroes    []PlayerHeroRow
	Equipment []PlayerEquipmentRow
}

type PlayerTroopRow struct {
	PlayerTag          string
	Name               string
	Level              int
	MaxLevel           int
	Village            string
	SuperTroopIsActive bool
}

type PlayerSpellRow struct {
	PlayerTag string
	Name      string
	Level     int
	MaxLevel  int
	Village   string
}

type PlayerHeroRow struct {
	PlayerTag string
	Name      string
	Level     int
	MaxLevel  int
	Village   string
}

type PlayerEquipmentRow struct {
	PlayerTag string
	Name      string
	Level     int
	MaxLevel  int
	Village   string
	Rarity    string
}
