import asyncio
from utility.config import Config

from bot.player.track import main as bot_player_main
from bot.clan.track import main as bot_clan_main
from bot.legends.track import main as bot_legend_main
from bot.reddit import main as bot_reddit_main

from gamewide.clan_verify.track import main as clan_verify_main
from gamewide.players.track import broadcast as global_player_main
from gamewide.scheduled.track import main as global_scheduled_main
from gamewide.war.track import main as global_war_main


if __name__ == "__main__":
    config = Config()
    if config.tracking_type == "BOTPLAYER":
        task = bot_player_main
    elif config.tracking_type == "BOTCLAN":
        task = bot_clan_main
    elif config.tracking_type == "CLANVERIFY":
        task = clan_verify_main
    elif config.tracking_type == "BOTREDDIT":
        task = bot_reddit_main
    elif config.tracking_type == "GLOBALPLAYER":
        task = global_player_main
    elif config.tracking_type == "BOTLEGENDS":
        task = bot_legend_main
    elif config.tracking_type == "GLOBALSCHEDULED":
        task = global_scheduled_main
    elif config.tracking_type == "GLOBALWAR":
        task = global_war_main
    else:
        task = asyncio.sleep

    loop = asyncio.get_event_loop()
    loop.create_task(task())
    loop.run_forever()
