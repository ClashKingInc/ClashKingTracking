import asyncio
from utility.config import Config

from bot.player.track import main as bot_player_main
from bot.clan.track import main as bot_clan_main
from bot.legends.track import main as bot_legend_main

from gamewide.clan_verify.track import main as clan_verify_main
from gamewide.players.track import broadcast as global_player_main

'''
11-20 - player tracking
21-25 - clan finder
26-30 - scheduled
31 - 40 - war finder
41 - 45 - bot tracking
46- 50 - clan verifier
'''

if __name__ == "__main__":
    config = Config()
    if config.tracking_type == "BOTPLAYER":
        task = bot_player_main
    elif config.tracking_type == "BOTCLAN":
        task = bot_clan_main
    elif config.tracking_type == "CLANVERIFY":
        task = clan_verify_main
    elif config.tracking_type == "GLOBALPLAYER":
        task = global_player_main
    elif config.tracking_type == "BOTLEGENDS":
        task = bot_legend_main
    else:
        task = asyncio.sleep(10)

    loop = asyncio.get_event_loop()
    loop.create_task(task())
    loop.run_forever()
