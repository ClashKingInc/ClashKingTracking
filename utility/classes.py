import motor.motor_asyncio


class MongoDatabase:
    def __init__(self, stats_db_connection, static_db_connection):
        self.stats_client = motor.motor_asyncio.AsyncIOMotorClient(
            stats_db_connection, compressors='snappy'
        )
        self.static_client = motor.motor_asyncio.AsyncIOMotorClient(
            static_db_connection
        )

        # databases - stats
        self.new_looper = self.stats_client.get_database('new_looper')
        self.looper = self.stats_client.get_database('looper')
        self.clashking = self.stats_client.get_database('clashking')
        self.ranking_history = self.stats_client.get_database(
            'ranking_history'
        )
        self.cache = self.stats_client.get_database('cache')
        self.leaderboards = self.stats_client.get_database('leaderboards')

        # databases - static
        self.usafam = self.static_client.get_database('usafam')

        # class typing creation, hacky but oh well
        # turns out this is from using dot notation, .get_ will be typed,
        # may switch in future
        self.clans_db = self.usafam.get_collection('clans')
        collection_class = self.clans_db.__class__

        # collections - stats
        self.player_stats: collection_class = self.new_looper.player_stats
        self.clan_stats: collection_class = self.new_looper.clan_stats
        self.player_history: collection_class = self.new_looper.player_history
        self.clan_wars: collection_class = self.looper.clan_war
        self.cwl_group: collection_class = self.looper.cwl_group
        self.basic_clan: collection_class = self.looper.clan_tags

        self.global_clans: collection_class = self.looper.clan_tags

        self.deleted_clans: collection_class = self.new_looper.deleted_clans
        self.join_leave_history: collection_class = (
            self.looper.join_leave_history
        )
        self.global_players: collection_class = self.clashking.global_players
        self.giveaways: collection_class = self.clashking.giveaways
        self.raid_weekends: collection_class = self.looper.raid_weekends
        self.war_timer: collection_class = self.looper.war_timer
        self.legend_history: collection_class = self.looper.legend_history

        self.player_trophies: collection_class = (
            self.ranking_history.player_trophies
        )
        self.player_versus_trophies: collection_class = (
            self.ranking_history.player_versus_trophies
        )
        self.clan_trophies: collection_class = (
            self.ranking_history.clan_trophies
        )
        self.clan_versus_trophies: collection_class = (
            self.ranking_history.clan_versus_trophies
        )
        self.capital: collection_class = self.ranking_history.capital

        self.capital_cache: collection_class = self.cache.capital_raids

        self.player_capital_lb: collection_class = (
            self.leaderboards.capital_player
        )
        self.clan_capital_lb: collection_class = self.leaderboards.capital_clan
        self.region_leaderboard: collection_class = (
            self.new_looper.leaderboard_db
        )

        # collections - static
        self.clans_db: collection_class = self.usafam.clans
        self.player_search: collection_class = self.usafam.player_search
        self.server_db: collection_class = self.usafam.server
        self.player_autocomplete: collection_class = self.usafam.player_search
        self.reminders: collection_class = self.usafam.reminders
