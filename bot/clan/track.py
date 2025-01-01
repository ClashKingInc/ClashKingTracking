import asyncio
import sentry_sdk
from kafka import KafkaProducer
from sentry_sdk.integrations.asyncio import AsyncioIntegration

from bot.dev.kafka_mock import MockKafkaProducer
from utility.utils import initialize_coc_client, sentry_filter
from utility.classes import MongoDatabase
from utility.config import Config
import pendulum as pend
import ujson
import time


class ClanTracker:
    """Class to manage clan tracking."""

    def __init__(self, config, producer=None, max_concurrent_requests=1000, max_requests_per_second=1000):
        self.config = config
        self.producer = producer or KafkaProducer(bootstrap_servers=["85.10.200.219:9092"])
        self.db_client = MongoDatabase(
            stats_db_connection=config.stats_mongodb,
            static_db_connection=config.static_mongodb,
        )
        self.coc_client = None
        self.clan_cache = {}  # Cache for tracking clan states
        self.last_private_warlog_warn = {}  # Cache for private war log warnings
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.message_count = 0  # Initialize the message counter

    async def initialize(self):
        """Initialize the CoC client."""
        self.coc_client = self.config.coc_client
        if not self.coc_client:
            raise RuntimeError("CoC client is not initialized in config.")

    async def track_batch(self, batch):
        """Track a batch of clans."""
        print(f"Tracking batch of {len(batch)} clans.")  # Added print
        async with self.semaphore:
            tasks = [self.track_clan(clan_tag) for clan_tag in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    self._handle_exception("Error in tracking task", result)
        print(f"Finished tracking batch of {len(batch)} clans.")  # Added print

    async def track(self, clan_tags):
        """Track updates for all clans in batches."""
        self.message_count = 0  # Reset message count
        batch_size = 1000  # Adjust batch size based on available resources
        for i in range(0, len(clan_tags), batch_size):
            batch = clan_tags[i:i + batch_size]
            print(f"Processing batch {i // batch_size + 1} of {len(clan_tags) // batch_size + 1}.")
            await self.track_batch(batch)

        sentry_sdk.add_breadcrumb(message="Finished tracking all clans.", level="info")
        print("Finished tracking all clans.")

    async def track_clan(self, clan_tag: str):
        """Track updates for a specific clan."""
        sentry_sdk.set_context("clan_tracking", {"clan_tag": clan_tag})
        try:
            async with self.semaphore:
                clan = await self.coc_client.get_clan(tag=clan_tag)
        except Exception as e:
            self._handle_exception(f"Error fetching clan {clan_tag}", e)
            return

        previous_clan = self.clan_cache.get(clan.tag)
        self.clan_cache[clan.tag] = clan

        if previous_clan is None:
            return

        sentry_sdk.add_breadcrumb(message=f"Tracking clan: {clan_tag}", level="info")
        self._handle_private_warlog(clan)
        self._handle_attribute_changes(clan, previous_clan)
        self._handle_member_changes(clan, previous_clan)
        self._handle_donation_updates(clan, previous_clan)

    def _send_to_kafka(self, topic, key, data):
        """Helper to send data to Kafka."""
        sentry_sdk.add_breadcrumb(
            message=f"Sending data to Kafka: topic={topic}, key={key}",
            data={"data_preview": data if self.config.is_beta else "Data suppressed in production"},
            level="info",
        )
        self.producer.send(
            topic=topic,
            value=ujson.dumps(data).encode('utf-8'),
            key=key.encode('utf-8'),
            timestamp_ms=int(pend.now(tz=pend.UTC).timestamp() * 1000),
        )
        self.message_count += 1

    def _handle_exception(self, message, exception):
        """Handle exceptions by logging to Sentry and console."""
        sentry_sdk.capture_exception(exception)
        print(f"{message}: {exception}")

    def _handle_private_warlog(self, clan):
        """Handle cases where the war log is private."""
        now = pend.now(tz=pend.UTC)
        last_warn = self.last_private_warlog_warn.get(clan.tag)

        if not clan.public_war_log:
            if last_warn is None or (now - last_warn).total_seconds() >= 12 * 3600:
                self.last_private_warlog_warn[clan.tag] = now
                json_data = {'type': 'war_log_closed', 'clan': clan._raw_data}
                self._send_to_kafka('clan', clan.tag, json_data)

    def _handle_attribute_changes(self, clan, previous_clan):
        """Handle changes in clan attributes."""
        attributes = [
            'level',
            'type',
            'description',
            'location',
            'capital_league',
            'required_townhall',
            'required_trophies',
            'war_win_streak',
            'war_league',
            'member_count',
        ]
        changed_attributes = [
            attr for attr in attributes
            if getattr(clan, attr) != getattr(previous_clan, attr)
        ]

        if changed_attributes:
            json_data = {
                'types': changed_attributes,
                'old_clan': previous_clan._raw_data,
                'new_clan': clan._raw_data,
            }
            self._send_to_kafka('clan', clan.tag, json_data)

    def _handle_member_changes(self, clan, previous_clan):
        """Handle changes in clan membership."""
        current_members = clan.members_dict
        previous_members = previous_clan.members_dict

        members_joined = [n._raw_data for n in clan.members if n.tag not in previous_members]
        members_left = [m._raw_data for m in previous_clan.members if m.tag not in current_members]

        if members_joined or members_left:
            json_data = {
                'type': 'members_join_leave',
                'old_clan': previous_clan._raw_data,
                'new_clan': clan._raw_data,
                'joined': members_joined,
                'left': members_left,
            }
            self._send_to_kafka('clan', clan.tag, json_data)

    def _handle_donation_updates(self, clan, previous_clan):
        """Handle updates to member donations."""
        previous_donations = {n.tag: (n.donations, n.received) for n in previous_clan.members}

        for member in clan.members:
            if (donated := previous_donations.get(member.tag)) is not None:
                mem_donos, mem_received = donated
                if mem_donos < member.donations or mem_received < member.received:
                    json_data = {
                        'type': 'all_member_donations',
                        'old_clan': previous_clan._raw_data,
                        'new_clan': clan._raw_data,
                    }
                    self._send_to_kafka('clan', clan.tag, json_data)
                    break


async def main():
    """Main function for clan tracking."""
    config = Config(config_type="bot_clan")

    # Initialize the CoC client
    await config.initialize()

    # Initialize Sentry for error tracking
    sentry_sdk.init(
        dsn=config.sentry_dsn,
        traces_sample_rate=1.0,
        integrations=[AsyncioIntegration()],
        profiles_sample_rate=1.0,
        environment="production" if not config.is_beta else "beta",
        before_send=sentry_filter,
    )

    producer = MockKafkaProducer() if config.is_beta else KafkaProducer(bootstrap_servers=["85.10.200.219:9092"])

    tracker = ClanTracker(config, producer=producer)
    await tracker.initialize()  # Transfer the initialized coc_client

    try:
        clan_tags = await tracker.db_client.clans_db.distinct("tag")
        while True:
            start_time = pend.now(tz=pend.UTC)
            await tracker.track(clan_tags)
            elapsed_time = pend.now(tz=pend.UTC) - start_time
            print(f"Tracked all {len(clan_tags)} clans in {elapsed_time.in_seconds()} seconds.")
            print(f"Total messages sent: {tracker.message_count} which is {tracker.message_count / elapsed_time.in_seconds()} messages per second.")
            await asyncio.sleep(60 if config.is_beta else 0)
    except KeyboardInterrupt:
        print("Tracking interrupted by user.")
    finally:
        await tracker.coc_client.close()
        print("Tracking completed.")


if __name__ == "__main__":
    asyncio.run(main())