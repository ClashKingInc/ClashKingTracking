import asyncio

import coc
import pendulum as pend
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from expiring_dict import ExpiringDict

from tracking import Tracking
from utility.config import TrackingType

WAR_CACHE = ExpiringDict(max_len=1000, max_age_seconds=3600)


class WarTracker(Tracking):
    """Class to manage war tracking."""

    def __init__(
        self, tracker_type: TrackingType, max_concurrent_requests=1000
    ):
        # Call the parent class constructor
        super().__init__(
            max_concurrent_requests=max_concurrent_requests,
            tracker_type=tracker_type,
        )
        self.scheduler = AsyncIOScheduler(timezone=pend.UTC)
        self.war_cache = WAR_CACHE

    async def _track_item(self, clan_tag):
        """Track updates for a specific clan's war, including CWL."""
        try:
            war = await self._fetch_current_war(clan_tag)
            cwl_next_round = await self._fetch_cwl_next_round(clan_tag, war)
            wars_to_track = [w for w in [war, cwl_next_round] if w is not None]

            for war in wars_to_track:
                if self._is_valid_war(war, clan_tag):
                    await self._process_war_data(clan_tag, war)
        except (coc.errors.PrivateWarLog, coc.errors.NotFound):
            pass
        except Exception as e:
            self._handle_exception(
                f'Error tracking war for clan {clan_tag}', e
            )

    async def _fetch_current_war(self, clan_tag):
        try:
            return await self.coc_client.get_current_war(clan_tag=clan_tag)
        except coc.errors.PrivateWarLog:
            return await self._fetch_opponent_war(clan_tag)

    async def _fetch_opponent_war(self, clan_tag):
        try:
            result = (
                await self.db_client.clan_wars.find(
                    {
                        '$and': [
                            {'clans': {'$in': [clan_tag]}},
                            {
                                'endTime': {
                                    '$gte': pend.now(tz=pend.UTC).int_timestamp
                                }
                            },
                        ]
                    }
                )
                .sort([('endTime', -1)])
                .to_list(length=None)
            )

            if not result:
                return None

            opponent_tag = next(
                (tag for tag in result[0].get('clans', []) if tag != clan_tag),
                None,
            )
            if not opponent_tag:
                return None

            try:
                return await self.coc_client.get_current_war(
                    clan_tag=opponent_tag
                )
            except coc.errors.PrivateWarLog:
                return None
            except coc.errors.NotFound:
                self.logger.error(
                    f'Opponent {opponent_tag} not found or no active war. Skipping.'
                )
                return None
        except Exception as e:
            self.logger.error(
                f"Error fetching opponent's war for clan {clan_tag}: {e}"
            )
            return None

    async def _fetch_cwl_next_round(self, clan_tag, war):
        if war and war.is_cwl:
            try:
                return await self.coc_client.get_current_war(
                    clan_tag=clan_tag,
                    cwl_round=coc.WarRound.current_preparation,
                )
            except Exception as e:
                self.logger.error(
                    f'Error fetching CWL next round for clan {clan_tag}: {e}'
                )
        return None

    def _is_valid_war(self, war, clan_tag):
        if war.state == 'notInWar':
            return False
        if not war.preparation_start_time or not war.end_time:
            self.logger.error(
                f'Incomplete war data for clan {clan_tag}. Preparation or end time missing. Skipping.'
            )
            return False
        return True

    async def _process_war_data(self, clan_tag, war):
        war_unique_id = self._generate_war_unique_id(war, clan_tag)
        previous_war = self.war_cache.get(war_unique_id)
        self.war_cache[war_unique_id] = war

        if self.is_first_iteration:
            return

        if previous_war is None or war._raw_data != previous_war._raw_data:
            await self._process_war_changes(clan_tag, war, previous_war)

        if war.is_cwl and previous_war:
            await self._process_cwl_changes(war, previous_war)

    @staticmethod
    def _generate_war_unique_id(war, clan_tag):
        if war.is_cwl:
            return f"{'-'.join([war.clan.tag, war.opponent.tag])}-{int(war.preparation_start_time.time.timestamp())}"
        return clan_tag

    async def _process_war_changes(self, clan_tag, current_war, previous_war):
        """Process changes between the current and previous wars."""
        if previous_war is None:
            # First-time war tracking
            self._send_to_kafka(
                'war',
                clan_tag,
                {
                    'type': 'new_war',
                    'clan_tag': clan_tag,
                    'war': current_war._raw_data,
                },
            )
        else:
            # Detect changes in the war state
            if current_war.state != previous_war.state:
                self._send_to_kafka(
                    'war',
                    clan_tag,
                    {
                        'type': 'war_state_change',
                        'clan_tag': clan_tag,
                        'old_state': previous_war.state,
                        'new_state': current_war.state,
                    },
                )

        # Detect new attacks or contributions
        await self._detect_member_contributions(
            clan_tag, current_war, previous_war
        )

        # Schedule reminders for the war
        if current_war:
            reminder_data = {
                'end_time': int(current_war.end_time.time.timestamp()),
                'state': current_war.state,
                'war_data': current_war._raw_data,
            }

            await self._schedule_reminders(
                clan_tag=clan_tag,
                war_tag=current_war.war_tag if current_war.war_tag else None,
                reminder_data=reminder_data,
                reminder_type='War',
            )

    async def _process_cwl_changes(self, war, previous_war):
        """Handle changes specific to CWL wars."""
        # Detect lineup changes
        added_members = [
            member._raw_data
            for member in war.clan.members
            if member.tag not in {m.tag for m in previous_war.clan.members}
        ]
        removed_members = [
            member._raw_data
            for member in previous_war.clan.members
            if member.tag not in {m.tag for m in war.clan.members}
        ]

        if added_members or removed_members:
            self._send_to_kafka(
                'cwl_changes',
                war.clan.tag,
                {
                    'type': 'cwl_lineup_change',
                    'added': added_members,
                    'removed': removed_members,
                    'war': war._raw_data,
                },
            )

    async def _detect_member_contributions(
        self, clan_tag, current_war, previous_war
    ):
        """Detect member contributions in the war and send updates to Kafka."""
        if current_war.attacks:
            # Compare attacks to find new ones
            new_attacks = [
                attack
                for attack in current_war.attacks
                if previous_war is None or attack not in previous_war.attacks
            ]

            if new_attacks:
                # Send new attacks to Kafka
                self._send_to_kafka(
                    'war',
                    clan_tag,
                    {
                        'type': 'new_attacks',
                        'clan_tag': clan_tag,
                        'attacks': [
                            attack._raw_data for attack in new_attacks
                        ],
                    },
                )


if __name__ == '__main__':
    # Create an instance of WarTracker and start the tracking loop
    tracker = WarTracker(tracker_type=TrackingType.BOT_WAR)
    asyncio.run(
        tracker.run(
            tracker_class=WarTracker,
            loop_interval=20,
        )
    )
