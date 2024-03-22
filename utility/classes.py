import motor.motor_asyncio

class MongoDatabase():
    def __init__(self, stats_db_connection, static_db_connection):
        self.stats_client = motor.motor_asyncio.AsyncIOMotorClient(stats_db_connection)
        self.static_client = motor.motor_asyncio.AsyncIOMotorClient(static_db_connection)

        #databases - stats
        self.new_looper = self.stats_client.get_database("new_looper")
        self.looper = self.stats_client.get_database("looper")
        self.clashking = self.stats_client.get_database("clashking")

        #databases - static
        self.usafam = self.static_client.get_database("usafam")

        #class typing creation, hacky but oh well
        #turns out this is from using dot notation, .get_ will be typed,
        #may switch in future
        self.clans_db = self.usafam.get_collection("clans")
        collection_class = self.clans_db.__class__


        #collections - stats
        self.player_stats: collection_class = self.new_looper.player_stats
        self.clan_stats: collection_class = self.new_looper.clan_stats
        self.player_history: collection_class = self.new_looper.player_history
        self.clan_wars: collection_class = self.looper.clan_war
        self.global_clans: collection_class = self.looper.clan_tags
        self.deleted_clans: collection_class = self.new_looper.deleted_clans
        self.join_leave_history: collection_class = self.looper.join_leave_history
        self.global_players: collection_class = self.clashking.global_players

        #collections - static
        self.clans_db: collection_class = self.usafam.clans
        self.player_search: collection_class = self.usafam.player_search
        self.server_db: collection_class = self.usafam.server

