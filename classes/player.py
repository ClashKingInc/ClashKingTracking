from msgspec import Struct
from typing import Union, List, Optional


class Achievement(Struct, frozen=True):
    name: str
    stars: int
    value: int
    target: int
    info: str
    completionInfo: Union[str, None]
    village: str

class BadgeUrls(Struct, frozen=True):
    small: str
    medium: str
    large: str

class Clan(Struct, frozen=True):
    tag: str
    name: str
    clanLevel: int
    badgeUrls: BadgeUrls

class Troop(Struct, frozen=True):
    name: str
    level: int
    maxLevel: int
    village: str


class Player(Struct, frozen=True, dict=True):
    tag: str
    name: str
    townHallLevel: int
    expLevel: int
    trophies: int
    bestTrophies: int
    warStars: int
    attackWins: int
    defenseWins: int
    builderBaseTrophies: int
    bestBuilderBaseTrophies: int
    donations: int
    donationsReceived: int
    clanCapitalContributions: int
    achievements: List[Achievement]
    heroes: List[Troop]
    spells: List[Troop]
    troops: List[Troop]
    role: Optional[str] = None
    warPreference: Optional[str] = None
    clan: Optional[Clan] = None

