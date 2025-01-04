import asyncio
import json

import coc
import pendulum as pend
import ujson
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
            await self._schedule_reminders(clan_tag, current_war)

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

    """async def _schedule_reminders(self, clan_tag, war):
        ""Schedule reminders for the war end time.""
        try:
            # Query the database for predefined reminders for this clan and war type
            set_times = await self.db_client.reminders.distinct(
                'time',
                filter={'$and': [{'clan': clan_tag}, {'type': 'War'}]},
            )

            if not set_times:
                return

            # Convert reminder times to seconds and prepare valid job IDs
            set_times_in_seconds = [
                int(float(r_time.replace(' hr', '')) * 3600)
                for r_time in set_times
            ]
            valid_job_ids = {
                f'war_reminder_{clan_tag}_{war.war_tag}_{time_seconds}'
                for time_seconds in set_times_in_seconds
            }

            # Get all existing jobs for this clan from the scheduler
            existing_jobs = {
                job.id: job
                for job in self.scheduler.get_jobs()
                if job.id.startswith(f'war_reminder_{clan_tag}_{war.war_tag}_')
            }

            # Remove jobs that no longer exist in the database
            outdated_jobs = set(existing_jobs) - valid_job_ids
            for job_id in outdated_jobs:
                self.logger.info(f'Removing outdated job: {job_id}')
                self.scheduler.remove_job(job_id)

            # Iterate through all reminder times
            for time_seconds in set_times_in_seconds:
                # Calculate the reminder time (X seconds before war.end_time)
                reminder_time = pend.instance(war.end_time.time).subtract(
                    seconds=time_seconds
                )
                job_id = (
                    f'war_reminder_{clan_tag}_{war.war_tag}_{time_seconds}'
                )
                existing_job = existing_jobs.get(job_id)

                if reminder_time <= pend.now(tz=pend.UTC):
                    continue  # Skip past reminders

                if existing_job:
                    if hasattr(existing_job.trigger, 'run_date'):
                        existing_run_date = (
                            existing_job.trigger.run_date.astimezone(pend.UTC)
                        )
                        time_diff = abs(
                            existing_run_date - reminder_time
                        ).total_seconds()
                        if time_diff <= 60:
                            # Skip if the time difference is within ±1 minute
                            continue
                        else:
                            # Remove and recreate the job if the difference is greater than 1 minute
                            self.logger.info(
                                f'Updating job {job_id} run_date from {existing_run_date} to {reminder_time}.'
                            )
                            self.scheduler.remove_job(job_id)

                # Schedule the reminder only if the time is in the future
                self.scheduler.add_job(
                    self._send_reminder,
                    'date',
                    args=[clan_tag, war],
                    id=job_id,
                    run_date=reminder_time,  # Explicitly set the `run_date` to match `reminder_time`
                    misfire_grace_time=600,
                )
                # self.logger.info(
                #     f'Scheduled reminder for clan {clan_tag} at {reminder_time} ({time_seconds} sec).'
                #)

        except Exception as e:
            self.logger.error(
                f'Error scheduling reminders for clan {clan_tag}: {e}'
            )"""

    async def _schedule_reminders(self, clan_tag, war):
        """Schedule reminders for the war end time using Redis."""
        try:
            # Query the database for predefined reminders for this clan and war type
            set_times = await self.db_client.reminders.distinct(
                'time',
                filter={'$and': [{'clan': clan_tag}, {'type': 'War'}]},
            )

            if not set_times:
                return

            # Convert reminder times to seconds
            set_times_in_seconds = [
                int(float(r_time.replace(' hr', '')) * 3600)
                for r_time in set_times
            ]

            # Iterate through all reminder times
            for time_seconds in set_times_in_seconds:
                # Calculate the reminder time (X seconds before war.end_time)
                reminder_time = (
                    pend.instance(war.end_time.time)
                    .subtract(seconds=time_seconds)
                    .int_timestamp
                )
                if war.war_tag:
                    job_id = (
                        f'war_reminder_{clan_tag}_{war.war_tag}_{time_seconds}'
                    )
                else:
                    job_id = f'war_reminder_{clan_tag}_{time_seconds}'

                # Skip past reminders
                if reminder_time <= pend.now(tz=pend.UTC).int_timestamp:
                    # Delete the outdated reminder from Redis if it exists
                    if await self.redis.get(job_id):
                        self.logger.info(
                            f'Deleting outdated reminder for job {job_id}. Reminder time: {pend.from_timestamp(reminder_time)}'
                        )
                        await self.redis.delete(job_id)
                    continue

                # Retrieve the existing reminder from Redis
                existing_reminder = await self.redis.get(job_id)
                if existing_reminder:
                    existing_reminder = ujson.loads(existing_reminder)
                    if existing_reminder['run_date'] == reminder_time:
                        # Skip if the reminder is up-to-date
                        continue
                    else:
                        self.logger.info(
                            f"Updating reminder for job {job_id}: old run_date={existing_reminder['run_date']}, new run_date={reminder_time}"
                        )

                # Prepare the reminder data
                reminder_data = {
                    'type': 'war_reminder',
                    'job_id': job_id,
                    'clan_tag': clan_tag,
                    'run_date': reminder_time,
                    'war_data': war._raw_data,
                }

                # Add or update the reminder in Redis
                await self.redis.set(job_id, ujson.dumps(reminder_data))
                # Set the expiration to 5 minutes after the `reminder_time`
                await self.redis.expireat(job_id, reminder_time + 300)

                self.logger.info(
                    f'Scheduled reminder for clan {clan_tag} at {pend.from_timestamp(reminder_time)} ({time_seconds} sec).'
                )

        except Exception as e:
            self.logger.error(
                f'Error scheduling reminders for clan {clan_tag}: {e}'
            )

    def _send_reminder(self, clan_tag, war):
        """Send a reminder about the war."""
        json_data = {
            'type': 'war_reminder',
            'clan_tag': clan_tag,
            'time_remaining': war.end_time.time.diff(
                pend.now(tz=pend.UTC)
            ).in_words(),
            'war': war._raw_data,
        }
        # Send the reminder to Kafka
        self._send_to_kafka('reminder', clan_tag, json_data)


if __name__ == '__main__':
    # Create an instance of WarTracker and start the tracking loop
    tracker = WarTracker(tracker_type=TrackingType.BOT_WAR)
    asyncio.run(
        tracker.run(
            tracker_class=WarTracker,
            loop_interval=20,
        )
    )
