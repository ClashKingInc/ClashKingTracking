import coc
import asyncio
from utility.utils import is_raid_tracking_time
from tracking import Tracking, main


class RaidTracker(Tracking):
    """Class to manage raid weekend tracking."""

    def __init__(self, config, producer=None, max_concurrent_requests=1000):
        super().__init__(config, producer, max_concurrent_requests)

    async def _track_item(self, clan_tag):
        """Track updates for a specific clan's raid."""
        try:
            raid_log = await self.coc_client.get_raid_log(clan_tag=clan_tag, limit=1)
            print(f"Tracking raid for clan: {clan_tag}")
        except Exception as e:
            self._handle_exception(f"Error tracking raid for clan {clan_tag}", e)

    async def _get_current_raid(self, clan_tag: str):
        """Get the current raid for a clan."""
        try:
            raid_log = await self.coc_client.get_raid_log(clan_tag=clan_tag, limit=1)
            return raid_log[0] if raid_log else None
        except coc.errors.NotFound:
            return None
        except Exception as e:
            self._handle_exception(f"Error fetching current raid for clan {clan_tag}", e)
            return None

    async def _get_previous_raid(self, clan_tag: str):
        """Get the previous raid for a clan from the database."""
        cached_raid = await self.db_client.capital_cache.find_one({"tag": clan_tag})
        if cached_raid and "data" in cached_raid:
            return coc.RaidLogEntry(data=cached_raid["data"], client=self.coc_client, clan_tag=clan_tag)
        return None

    async def _process_raid_changes(self, clan_tag: str, current_raid: coc.RaidLogEntry,
                                    previous_raid: coc.RaidLogEntry):
        """Process changes between the current and previous raids."""
        if previous_raid and current_raid._raw_data == previous_raid._raw_data:
            return  # No changes

        # Update the database with the current raid
        await self.db_client.capital_cache.update_one(
            {"tag": clan_tag}, {"$set": {"data": current_raid._raw_data}}, upsert=True
        )

        if previous_raid:
            await self._detect_new_opponents(clan_tag, current_raid, previous_raid)
            await self._detect_raid_state_changes(clan_tag, current_raid, previous_raid)
            await self._detect_member_attacks(clan_tag, current_raid, previous_raid)

    async def _detect_new_opponents(self, clan_tag: str, current_raid: coc.RaidLogEntry,
                                    previous_raid: coc.RaidLogEntry):
        """Detect new offensive opponents and send to Kafka."""
        new_opponents = (
            clan for clan in current_raid.attack_log if clan not in previous_raid.attack_log
        ) if previous_raid.attack_log else current_raid.attack_log

        for clan in new_opponents:
            json_data = {
                "type": "new_offensive_opponent",
                "clan": clan._raw_data,
                "clan_tag": clan_tag,
                "raid": current_raid._raw_data,
            }
            self._send_to_kafka("capital", clan_tag, json_data)

    async def _detect_raid_state_changes(self, clan_tag: str, current_raid: coc.RaidLogEntry,
                                         previous_raid: coc.RaidLogEntry):
        """Detect changes in the raid state and send to Kafka."""
        if current_raid.state != previous_raid.state:
            json_data = {
                "type": "raid_state",
                "clan_tag": current_raid.clan_tag,
                "old_raid": previous_raid._raw_data,
                "raid": current_raid._raw_data,
            }
            self._send_to_kafka("capital", clan_tag, json_data)

    async def _detect_member_attacks(self, clan_tag: str, current_raid: coc.RaidLogEntry,
                                     previous_raid: coc.RaidLogEntry):
        """Detect member attack changes and send to Kafka."""
        attacked = []
        for member in current_raid.members:
            old_member = coc.utils.get(previous_raid.members, tag=member.tag)
            if old_member is None or old_member.attack_count != member.attack_count:
                attacked.append(member.tag)

        if attacked:
            json_data = {
                "type": "raid_attacks",
                "clan_tag": current_raid.clan_tag,
                "attacked": attacked,
                "raid": current_raid._raw_data,
                "old_raid": previous_raid._raw_data,
            }
            self._send_to_kafka("capital", clan_tag, json_data)


if __name__ == "__main__":
    asyncio.run(main(tracker_class=RaidTracker, config_type="bot_clan", is_tracking_allowed=is_raid_tracking_time, loop_interval=20))
