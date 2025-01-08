import asyncio
from collections import defaultdict, deque
from enum import Enum

import aiohttp
import coc
import pendulum as pend
import sentry_sdk
import ujson
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from asyncio_throttle import Throttler
from bson import ObjectId
from loguru import logger
from pymongo import ReturnDocument
from sentry_sdk.integrations.asyncio import AsyncioIntegration

from utility.classes_utils.config_utils import sentry_filter
from utility.classes_utils.database_utils import generate_custom_id
from utility.config import Config


class Tracking:
    def __init__(
        self,
        tracker_type,
        max_concurrent_requests=1000,
        batch_size=500,
        throttle_speed=1000,
    ):
        self.keys = None
        self.config = Config(config_type=tracker_type)
        self.db_client = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.message_count = 0
        self.iterations = 0
        self.is_first_iteration = True
        self.batch_size = batch_size
        self.throttler = Throttler(throttle_speed)
        self.coc_client = None
        self.redis = None
        self.logger = logger
        self.http_session = None
        self.scheduler = None
        self.kafka = None
        self.type = tracker_type
        self.max_stats_size = 10_000
        self.request_stats = defaultdict(
            lambda: deque(maxlen=self.max_stats_size)
        )

    async def initialize(self):
        """Initialise the tracker with dependencies."""
        await self.config.initialize()
        self.db_client = self.config.get_mongo_database()
        self.redis = self.config.get_redis_client()
        self.coc_client = self.config.coc_client

        self.kafka = self.config.get_kafka_producer()

        connector = aiohttp.TCPConnector(limit=1200, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=1800)
        self.http_session = aiohttp.ClientSession(
            connector=connector, timeout=timeout, json_serialize=ujson.dumps
        )

        self.scheduler = AsyncIOScheduler(timezone=pend.UTC)

    async def track(self, items):
        """Track items in batches."""
        self.message_count = 0  # Reset message count
        for i in range(0, len(items), self.batch_size):
            batch = items[i : i + self.batch_size]
            self.logger.info(
                f'Processing batch {i // self.batch_size + 1} of {len(items) // self.batch_size + 1}.'
            )
            await self._track_batch(batch)

        sentry_sdk.add_breadcrumb(
            message='Finished tracking all clans.', level='info'
        )
        self.logger.info('Finished tracking all clans.')

    async def fetch(self, url: str, tag: str, json=False):
        async with self.throttler:
            self.keys.rotate(1)
            self.request_stats[url].append(
                {'time': pend.now(tz=pend.UTC).timestamp()}
            )
            async with self.http_session.get(
                url, headers={'Authorization': f'Bearer {self.keys[0]}'}
            ) as response:
                if response.status == 200:
                    if not json:
                        return (await response.read(), tag)
                    return (await response.json(), tag)
                return (None, None)

    async def _track_batch(self, batch):
        """Track a batch of items."""
        async with self.semaphore:
            tasks = [self._track_item(item) for item in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    self._handle_exception('Error in tracking task', result)
        self.logger.info(f'Finished tracking batch of {len(batch)} clans.')

    async def _track_item(self, item):
        """Override this method in child classes_utils."""
        raise NotImplementedError(
            'This method should be overridden in child classes_utils.'
        )

    async def _schedule_reminder(self, job_id, reminder_document):
        """Schedule a new reminder by inserting it into MongoDB.

        Args:
            job_id (str): Unique ID for the reminder.
            reminder_document (dict): Document data for the reminder.
        """
        try:
            result = await self.db_client.active_reminders.insert_one(
                reminder_document
            )
            self.logger.info(
                f'New reminder scheduled: {job_id}, Inserted ID: {result.inserted_id}'
            )
        except Exception as e:
            self.logger.error(f'Error scheduling reminder {job_id}: {e}')

    async def _update_reminder(self, job_id, reminder_document):
        """Update an existing reminder in MongoDB.

        Args:
            job_id (str): Unique ID for the reminder.
            reminder_document (dict): Document data for the reminder.
        """
        try:
            self.logger.info(f'Updating existing reminder: {job_id}')
            # Update the reminder in the `active_reminders` collection
            result = await self.db_client.active_reminders.find_one_and_update(
                {'job_id': job_id},
                {'$set': reminder_document},
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
            if result:
                self.logger.info(
                    f'Reminder successfully updated: {job_id}, Result: {result}'
                )
            else:
                self.logger.warning(
                    f'No document updated for job_id: {job_id}'
                )
            return result
        except Exception as e:
            self.logger.error(f'Error updating reminder {job_id}: {e}')

    async def _schedule_reminders(
        self, clan_tag, war_tag, reminder_data, reminder_type
    ):
        """Schedule reminders for various entities using MongoDB.

        Args:
            clan_tag (str): Clan tag.
            war_tag (str): War tag.
            reminder_data (dict): Data specific to the entity (e.g., war data).
            reminder_type (str): Type of the reminder (e.g., 'war_reminder', 'raid_reminder').
        """
        try:
            # Query the database for predefined reminder settings
            reminders = await self.db_client.reminders.find(
                {'$and': [{'clan': clan_tag}, {'type': reminder_type}]},
                {'time': 1, '_id': 1},  # Only retrieve 'time' and '_id'
            ).to_list(length=None)

            if not reminders:
                return

            # Convert reminder times to seconds and pair with their respective `_id`
            set_times_with_ids = [
                (
                    int(float(reminder['time'].replace(' hr', '')) * 3600),
                    str(reminder['_id']),
                )
                for reminder in reminders
            ]

            if war_tag:
                entity_tag = f'{clan_tag}_{war_tag}'
            else:
                entity_tag = clan_tag

            # Iterate through all reminder times
            for time_seconds, reminder_id in set_times_with_ids:
                # Calculate the reminder time
                target_time = pend.from_timestamp(
                    reminder_data['end_time'], tz=pend.UTC
                ).subtract(seconds=time_seconds)
                reminder_time = target_time.int_timestamp

                job_id = f'{reminder_type}_{entity_tag}_{time_seconds}'

                # Skip past reminders
                if reminder_time <= pend.now(tz=pend.UTC).int_timestamp:
                    # Delete outdated reminders
                    deleted = await self.db_client.active_reminders.find_one_and_delete(
                        {'job_id': job_id}
                    )
                    if deleted:
                        self.logger.info(
                            f'Deleted outdated reminder: {job_id}'
                        )
                    continue

                # Check if the reminder already exists
                existing_reminder = (
                    await self.db_client.active_reminders.find_one(
                        {'job_id': job_id}
                    )
                )
                if existing_reminder:
                    # Prepare the reminder document
                    reminder_document = {
                        'job_id': job_id,
                        'type': reminder_type,
                        'run_date': reminder_time,
                        'reminder_id': ObjectId(reminder_id),
                    }
                    # Update the reminder
                    await self._update_reminder(job_id, reminder_document)
                else:
                    # Prepare the reminder document
                    reminder_document = {
                        '_id': generate_custom_id(reminder_time),
                        'job_id': job_id,
                        'type': reminder_type,
                        'run_date': reminder_time,
                        'reminder_id': ObjectId(reminder_id),
                    }
                    # Schedule a new reminder
                    await self._schedule_reminder(job_id, reminder_document)

                # Set an expiration time (5 minutes after the `run_date`)
                await self.db_client.active_reminders.update_one(
                    {'job_id': job_id},
                    {
                        '$set': {
                            'expireAt': pend.from_timestamp(
                                reminder_time + 300
                            ).isoformat()
                        }
                    },
                )

        except Exception as e:
            self.logger.error(
                f'Error scheduling reminders for {clan_tag} and {war_tag}: {e}'
            )

    def _send_to_kafka(self, topic, key, data):
        """Helper to send data to Kafka, handling non-JSON-serializable objects like enums."""

        def serialize(obj):
            """Convert objects like Enums to JSON-serializable values."""
            if isinstance(obj, Enum):
                return obj.value  # Use the value of the Enum
            return obj  # Return the object as-is if it's already serializable

        # Serialize the data dictionary
        serialized_data = {k: serialize(v) for k, v in data.items()}

        sentry_sdk.add_breadcrumb(
            message=f'Sending data to Kafka: topic={topic}, key={key}',
            data={
                'data_preview': serialized_data
                if self.config.is_beta
                else 'Data suppressed in production'
            },
            level='info',
        )

        # Send the serialized data to Kafka
        self.kafka.send(
            topic=topic,
            value=ujson.dumps(serialized_data).encode('utf-8'),
            key=key.encode('utf-8'),
            timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
        )
        self.message_count += 1

    @staticmethod
    def _handle_exception(message, exception):
        """Handle exceptions by logging to Sentry and console."""
        sentry_sdk.capture_exception(exception)

    @staticmethod
    def gen_raid_date():
        now = pend.now(tz=pend.UTC)
        current_dayofweek = now.day_of_week  # Monday = 0, Sunday = 6
        if (
            (current_dayofweek == 4 and now.hour >= 7)  # Friday after 7 AM UTC
            or (current_dayofweek == 5)  # Saturday
            or (current_dayofweek == 6)  # Sunday
            or (
                current_dayofweek == 0 and now.hour < 7
            )  # Monday before 7 AM UTC
        ):
            raid_date = now.subtract(
                days=(current_dayofweek - 4 if current_dayofweek >= 4 else 0)
            ).date()
        else:
            forward = 4 - current_dayofweek  # Days until next Friday
            raid_date = now.add(days=forward).date()
        return str(raid_date)

    @staticmethod
    def gen_season_date():
        end = coc.utils.get_season_end().astimezone(pend.UTC)
        month = f'{end.month:02}'
        return f'{end.year}-{month}'

    @staticmethod
    def gen_legend_date():
        now = pend.now(tz=pend.UTC)
        date = now.subtract(days=1).date() if now.hour < 5 else now.date()
        return str(date)

    @staticmethod
    def gen_games_season():
        now = pend.now(tz=pend.UTC)
        month = f'{now.month:02}'  # Ensure two-digit month
        return f'{now.year}-{month}'

    @staticmethod
    def is_raids():
        """
        Check if the current time is within the raid tracking window (Friday 7:00 UTC to Monday 9:00 UTC).
        """
        now = pend.now('UTC')
        friday_7am = now.start_of('week').add(days=4, hours=7)
        monday_9am = now.start_of('week').add(days=7, hours=9)
        return friday_7am <= now < monday_9am

    async def run(
        self,
        tracker_class,
        loop_interval=60,
        is_tracking_allowed=None,
        use_scheduler=False,
        setup_scheduler_method=None,
    ):
        """
        Main function for generic tracking or scheduling.

        :param tracker_class: The tracker class to instantiate (e.g., ClanTracker, RaidTracker).
        :param loop_interval: The interval in seconds between tracking loops.
        :param is_tracking_allowed: A function that returns True if tracking should run, False otherwise.
        :param use_scheduler: If True, use a scheduler-based execution flow.
        :param setup_scheduler_method: Method to set up the scheduler, required if `use_scheduler` is True.
        """
        tracker = tracker_class(self.type)
        await tracker.initialize()

        sentry_sdk.init(
            dsn=tracker.config.sentry_dsn,
            traces_sample_rate=1.0,
            integrations=[AsyncioIntegration()],
            profiles_sample_rate=1.0,
            environment='production' if tracker.config.is_main else 'beta',
            before_send=sentry_filter,
        )

        try:
            if use_scheduler:
                if not setup_scheduler_method:
                    raise ValueError(
                        'A setup_scheduler_method must be provided for scheduler mode.'
                    )

                # Setup the scheduler
                setup_scheduler_method(tracker)
                tracker.scheduler.start()
                tracker.logger.info(
                    'Scheduler started. Running scheduled jobs...'
                )

                # Keep the application running
                while True:
                    await asyncio.sleep(3600)
            else:
                # Tracking loop
                async with tracker.http_session:
                    while True:
                        if (
                            is_tracking_allowed is None
                            or is_tracking_allowed()
                        ):
                            clan_tags = (
                                await tracker.db_client.clans_db.distinct(
                                    'tag'
                                )
                            )
                            start_time = pend.now(tz=pend.UTC)
                            await tracker.track(clan_tags)
                            elapsed_time = pend.now(tz=pend.UTC) - start_time
                            tracker.logger.info(
                                f'Tracked {len(clan_tags)} clans in {elapsed_time.in_seconds()} seconds. '
                                f'Messages sent: {tracker.message_count} '
                                f'({tracker.message_count / elapsed_time.in_seconds()} msg/s).'
                            )
                        else:
                            tracker.logger.info(
                                'Tracking not allowed. Sleeping until the next interval.'
                            )

                        if tracker.is_first_iteration:
                            tracker.is_first_iteration = False

                        await asyncio.sleep(loop_interval)
        except KeyboardInterrupt:
            tracker.logger.info('Execution interrupted by user.')
        except SystemExit:
            tracker.logger.info('Shutting down...')
        finally:
            # Clean up resources
            if tracker.db_client and hasattr(tracker.db_client, 'close'):
                await tracker.db_client.close()  # Ferme la connexion MongoDB
            if tracker.coc_client:
                await tracker.coc_client.close()
            if tracker.http_session and not tracker.http_session.closed:
                await tracker.http_session.close()
            if tracker.scheduler.running:
                tracker.scheduler.shutdown()
            tracker.logger.info('Resources cleaned up. Exiting...')
