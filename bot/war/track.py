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
            # Fetch the current war for the clan
            try:
                war = await self.coc_client.get_current_war(clan_tag=clan_tag)
            except coc.errors.PrivateWarLog:
                # Search the database for the latest war involving the clan
                try:
                    result = (
                        await self.db_client.clan_wars.find(
                            {
                                '$and': [
                                    {
                                        'clans': {'$in': [clan_tag]}
                                    },  # Clan tag is in the list of clans
                                    {
                                        'endTime': {
                                            '$gte': pend.now(
                                                tz=pend.UTC
                                            ).int_timestamp
                                        }
                                    },
                                    # War is ongoing or in prep
                                ]
                            }
                        )
                        .sort([('endTime', -1)])  # Last war first
                        .to_list(length=None)
                    )

                    if result:
                        clans = result[0].get(
                            'clans', []
                        )  # Get the list of clans for the last war
                        if not clans:
                            self.logger.warning(
                                f'No clans found in the result for clan {clan_tag}. Skipping.'
                            )
                            return

                        opponent_tag = [
                            tag for tag in clans if tag != clan_tag
                        ]  # Get the opponent's tag
                        if not opponent_tag:
                            self.logger.warning(
                                f'No opponent tag found for clan {clan_tag}. Skipping.'
                            )
                            return

                        opponent_tag = opponent_tag[
                            0
                        ]  # Safe to access the first element now
                        try:
                            war = await self.coc_client.get_current_war(
                                clan_tag=opponent_tag
                            )
                            # self.logger.info(f"Fetched opponent's war for clan {clan_tag} with opponent {opponent_tag}.")
                        except coc.errors.PrivateWarLog:
                            self.logger.warning(
                                f'Opponent {opponent_tag} also has a private war log. Skipping.'
                            )
                            return
                    else:
                        # self.logger.info(f"No recent wars found in the database for clan {clan_tag}. Skipping.")
                        return

                except Exception as e:
                    self.logger.error(
                        f"Error fetching opponent's war for clan {clan_tag}: {e}"
                    )
                    return

            # Handle CWL wars
            cwl_next_round = None
            if war and war.is_cwl:
                try:
                    cwl_next_round = await self.coc_client.get_current_war(
                        clan_tag=clan_tag,
                        cwl_round=coc.WarRound.current_preparation,
                    )
                except Exception as e:
                    self.logger.error(
                        f'Error fetching CWL next round for clan {clan_tag}: {e}'
                    )

            # Combine regular war and CWL next round
            wars_to_track = [w for w in [war, cwl_next_round] if w is not None]

            for war in wars_to_track:
                # Handle cases where the API returns None
                if war is None:
                    continue

                # Check if the clan is not in war
                if war.state == 'notInWar':
                    continue

                # Check for incomplete war data
                if war.preparation_start_time is None or war.end_time is None:
                    self.logger.error(
                        f'Incomplete war data for clan {clan_tag}. Preparation or end time missing. Skipping.'
                    )
                    continue

                # Generate a unique war ID for CWL wars
                war_unique_id = (
                    (
                        '-'.join([war.clan.tag, war.opponent.tag])
                        + f'-{int(war.preparation_start_time.time.timestamp())}'
                    )
                    if war.is_cwl
                    else clan_tag
                )

                # Retrieve the previously cached war data
                previous_war = self.war_cache.get(war_unique_id)

                # Cache the current war data
                self.war_cache[war_unique_id] = war

                if self.is_first_iteration:
                    continue

                # Process changes if this is the first time or if the war data has changed
                if (
                    previous_war is None
                    or war._raw_data != previous_war._raw_data
                ):
                    await self._process_war_changes(
                        clan_tag, war, previous_war
                    )

                # Handle CWL-specific changes
                if war.is_cwl and previous_war:
                    await self._process_cwl_changes(war, previous_war)

        except coc.errors.PrivateWarLog:
            pass
        except coc.errors.NotFound:
            pass
        except Exception as e:
            self._handle_exception(
                f'Error tracking war for clan {clan_tag}', e
            )

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
