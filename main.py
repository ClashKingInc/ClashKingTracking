import os
import asyncio
from dotenv import load_dotenv
load_dotenv()

import scripts
SCRIPT_NAME = os.getenv("SCRIPT_NAME")
main_function = ...


class_script_map = {
    "bot_clans": scripts.ClanTracker,
    "bot_players": scripts.PlayerTracking,
    "events_ws": scripts.TrackingWebsocket,
    "giveaways": scripts.GiveawayTracking,
    "global_clans": scripts.GlobalClanTracking,
    "global_war_store": scripts.GlobalWarStore,
    "global_war_track": scripts.GlobalWarTrack,
    "legends": scripts.LegendTracking,
    "reddit": scripts.RedditTracking,
    "scheduled": scripts.ScheduledTracking,
}
run_class = class_script_map[SCRIPT_NAME]
asyncio.run(run_class().run())