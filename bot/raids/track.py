import asyncio

import coc

from tracking import Tracking
from utility.config import TrackingType

# Global cache for clans
CLAN_CACHE = {}
EXISTS_KEY = '$exists'


class RaidTracker(Tracking):
    """Class to manage raid weekend tracking."""

    def __init__(
        self, tracker_type: TrackingType, max_concurrent_requests=1000
    ):
        # Call the parent class constructor
        super().__init__(
            max_concurrent_requests=max_concurrent_requests,
            tracker_type=tracker_type,
        )

    async def _track_item(self, clan_tag):
        """Track updates for a specific clan's raid."""
        try:
            current_raid = await self._get_current_raid(clan_tag)
            if not current_raid:
                return

            previous_raid = await self._get_previous_raid(clan_tag)
            await self._process_raid_changes(
                clan_tag, current_raid, previous_raid
            )

        except Exception as e:
            self._handle_exception(
                f'Error tracking raid for clan {clan_tag}', e
            )

    async def _get_current_raid(self, clan_tag: str):
        """Get the current raid for a clan."""
        try:
            raid_log = await self.coc_client.get_raid_log(
                clan_tag=clan_tag, limit=1
            )
            return raid_log[0] if raid_log else None
        except coc.errors.NotFound:
            return None
        except Exception as e:
            self._handle_exception(
                f'Error fetching current raid for clan {clan_tag}', e
            )
            return None

    async def _get_previous_raid(self, clan_tag: str):
        """Get the previous raid for a clan from the database."""
        cached_raid = await self.db_client.capital_cache.find_one(
            {'tag': clan_tag}
        )
        if cached_raid and 'data' in cached_raid:
            return coc.RaidLogEntry(
                data=cached_raid['data'],
                client=self.coc_client,
                clan_tag=clan_tag,
            )
        return None

    async def _process_raid_changes(
        self,
        clan_tag: str,
        current_raid: coc.RaidLogEntry,
        previous_raid: coc.RaidLogEntry,
    ):
        """Process changes between the current and previous raids."""
        if previous_raid and current_raid._raw_data == previous_raid._raw_data:
            return  # No changes

        # Update the database with the current raid
        await self.db_client.capital_cache.update_one(
            {'tag': clan_tag},
            {'$set': {'data': current_raid._raw_data}},
            upsert=True,
        )

        if previous_raid:
            await self._detect_new_opponents(
                clan_tag, current_raid, previous_raid
            )
            # await self._detect_raid_state_changes(
            #    clan_tag, current_raid, previous_raid
            # )
            await self._detect_member_attacks(
                clan_tag, current_raid, previous_raid
            )

        # Update the global clan cache
        CLAN_CACHE[clan_tag] = current_raid

    async def _detect_new_opponents(
        self,
        clan_tag: str,
        current_raid: coc.RaidLogEntry,
        previous_raid: coc.RaidLogEntry,
    ):
        """Detect new offensive opponents and send to Kafka."""
        new_opponents = (
            (
                clan
                for clan in current_raid.attack_log
                if clan not in previous_raid.attack_log
            )
            if previous_raid.attack_log
            else current_raid.attack_log
        )

        for clan in new_opponents:
            logs_settings = await self._check_raids_settings(
                clan_tag, 'new_raid_panel'
            )
            for logs_setting in logs_settings:
                json_data = {
                    'type': 'new_offensive_opponent',
                    'server': logs_setting['server'],
                    'webhook_id': logs_setting['new_raid_panel_webhook_id'],
                    'clan': clan._raw_data,
                    'clan_tag': clan_tag,
                    'previous_raid': previous_raid._raw_data,
                }
                await self._send_to_kafka('capital', clan_tag, json_data)

    async def _detect_member_attacks(
        self,
        clan_tag: str,
        current_raid: coc.RaidLogEntry,
        previous_raid: coc.RaidLogEntry,
    ):
        """
        Detect member attack changes and send detailed information to Kafka.

        Args:
            clan_tag (str): Tag of the clan.
            current_raid (coc.RaidLogEntry): The current raid log entry.
            previous_raid (coc.RaidLogEntry): The previous raid log entry.
        """
        try:
            detailed_attacks = []

            # Iterate over all districts in the current raid's attack log
            for current_district in current_raid._raw_data.get(
                'attackLog', []
            ):
                district_info = self._extract_district_info(current_district)
                if not district_info:
                    continue

                previous_district = self._find_previous_district(
                    previous_raid, district_info['district_tag']
                )

                cumulative_destruction = self._get_cumulative_destruction(
                    previous_district
                )

                new_attacks = self._process_attacks_in_district(
                    current_district,
                    previous_district,
                    district_info,
                    cumulative_destruction,
                )

                detailed_attacks.extend(new_attacks)

            # Send to Kafka if there are new attacks
            if detailed_attacks:
                await self._send_raid_attacks_to_kafka(
                    clan_tag, current_raid, detailed_attacks
                )

        except Exception as e:
            # Handle unexpected errors and log them
            self._handle_exception(
                f'Error detecting member attacks for clan {clan_tag}', e
            )

    @staticmethod
    def _extract_district_info(current_district):
        """Extract district information such as tag and name."""
        district_tag = current_district.get('defender', {}).get('tag')
        district_name = current_district.get('defender', {}).get('name')

        if not district_tag or not district_name:
            return None

        return {'district_tag': district_tag, 'district_name': district_name}

    @staticmethod
    def _find_previous_district(previous_raid, district_tag):
        """Find the corresponding district in the previous raid."""
        return next(
            (
                district
                for district in previous_raid._raw_data.get('attackLog', [])
                if district.get('defender', {}).get('tag') == district_tag
            ),
            None,
        )

    @staticmethod
    def _get_cumulative_destruction(previous_district):
        """Calculate the cumulative destruction from the previous district."""
        if not previous_district:
            return 0

        return max(
            attack.get('destructionPercent', 0)
            for district in previous_district.get('districts', [])
            for attack in district.get('attacks', [])
        )

    def _process_attacks_in_district(
        self,
        current_district,
        previous_district,
        district_info,
        cumulative_destruction,
    ):
        """Process each attack in the current district and determine if it's new."""
        detailed_attacks = []  # Store details of new attacks

        for current_attack_data in current_district.get('districts', []):
            total_attacks_on_district = current_attack_data.get(
                'attackCount', 0
            )

            # Iterate over all attacks in the district
            for attack_data in current_attack_data.get('attacks', []):
                attacker_tag = attack_data.get('attacker', {}).get('tag')
                attacker_name = attack_data.get('attacker', {}).get('name')
                destruction_percentage = attack_data.get(
                    'destructionPercent', 0
                )
                stars = attack_data.get('stars', 0)
                total_looted = current_attack_data.get('totalLooted', 0)

                if not attacker_tag or not attacker_name:
                    continue  # Skip invalid attacks

                # Calculate the contribution of this attack
                attack_contribution = max(
                    0, destruction_percentage - cumulative_destruction
                )
                cumulative_destruction = (
                    destruction_percentage  # Update cumulative destruction
                )

                # Check if the attack is new by comparing with the previous district
                if not self._is_new_attack(previous_district, attack_data):
                    continue

                # Add new attack details
                detailed_attacks.append(
                    {
                        'district_name': district_info['district_name'],
                        'district_tag': district_info['district_tag'],
                        'attacker_tag': attacker_tag,
                        'attacker_name': attacker_name,
                        'stars': stars,
                        'destruction_percentage': attack_contribution,
                        'cumulative_destruction_percentage': destruction_percentage,
                        'total_looted': total_looted,
                        'total_attacks_on_district': total_attacks_on_district,
                    }
                )

        return detailed_attacks

    @staticmethod
    def _is_new_attack(previous_district, attack_data):
        """Determine if an attack is new by checking the previous district's attacks."""
        if not previous_district:
            return True

        for prev_district in previous_district.get('districts', []):
            if attack_data in prev_district.get('attacks', []):
                return False

        return True

    async def _send_raid_attacks_to_kafka(
        self, clan_tag, current_raid, detailed_attacks
    ):
        """Send new attack data to Kafka."""
        logs_settings = await self._check_raids_settings(
            clan_tag, 'capital_attacks'
        )
        for logs_setting in logs_settings:
            json_data = {
                'type': 'raid_attacks',
                'clan_tag': current_raid.clan_tag,
                'server': logs_setting['server'],
                'webhook_id': logs_setting['capital_attacks_webhook_id'],
                'detailed_attacks': detailed_attacks,
            }
            await self._send_to_kafka('capital', clan_tag, json_data)

    async def _check_raids_settings(self, clan_tag, data):
        """Check the CWL database settings."""
        # Build the dynamic aggregation pipeline
        logs_data_key = f'logs.{data}'
        logs_data_webhook_key = f'logs.{data}.webhook'

        # Build the dynamic data key
        result = await self.db_client.clans_db.aggregate(
            [
                {
                    '$match': {
                        'tag': clan_tag,
                        'server': {EXISTS_KEY: True},
                        'logs': {EXISTS_KEY: True},
                        logs_data_key: {EXISTS_KEY: True},
                        logs_data_webhook_key: {EXISTS_KEY: True, '$ne': None},
                    }
                },
                {
                    '$project': {
                        '_id': 0,  # Exclude the default MongoDB `_id` field
                        'server': '$server',
                        f'{data}_webhook_id': f'${logs_data_webhook_key}',
                    }
                },
            ]
        ).to_list(length=None)
        return result


if __name__ == '__main__':
    tracker = RaidTracker(tracker_type=TrackingType.BOT_RAIDS)
    asyncio.run(
        tracker.run(
            tracker_class=RaidTracker,
            loop_interval=20,
            is_tracking_allowed=tracker.is_raids,
        )
    )
