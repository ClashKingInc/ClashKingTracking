from typing import List, Optional


class Clan:
    def __init__(self, raw_data: dict):
        self.name: str = raw_data.get("name")
        self.tag: str = raw_data.get("tag")


class Equipment:
    def __init__(self, raw_data: dict):
        self.name: str = raw_data.get("name")
        self.level: int = raw_data.get("level")


class Heroes:
    def __init__(self, raw_data: dict):
        self.name = raw_data.get("name")
        self.equipment: Optional[List[Equipment]] = [Equipment(raw_data=data) for data in raw_data.get("equipment")]


class League:
    def __init__(self, raw_data: dict):
        self.name = raw_data.get("name", "Unranked")


class Player:
    def __init__(self, raw_data: dict) -> None:
        self.raw_data: dict = raw_data
        self.name = raw_data.get("name")
        self.tag = raw_data.get("tag")
        self.trophies: int = raw_data.get("trophies")
        self.attackWins: int = raw_data.get("attackWins")
        self.defenseWins: int = raw_data.get("defenseWins")
        self.heroes: List[Heroes] = [Heroes(raw_data=data) for data in raw_data.get("heroes")]
        self.equipment: List[Equipment] = [Equipment(raw_data=data) for data in raw_data.get("heroEquipment")]
        self.clan: Clan = Clan(raw_data=raw_data.get("clan", {}))
        self.league: League = League(raw_data=raw_data.get("league"))
