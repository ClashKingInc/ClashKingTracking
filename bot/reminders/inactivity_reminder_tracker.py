import asyncio
import pendulum as pend
from loguru import logger
from bot.war.utils import reminder_times
from utility.config import Config, TrackingType
import json

from utility.utils import serialize


class InactivityReminderTracker:
    def __init__(self, config, db_client, kafka_producer):
        """Initialize the Inactivity Reminder Tracker."""
        self.logger = logger
        self.config = config
        self.db_client = db_client
        self.kafka_producer = kafka_producer
        self.coc_client = self.config.coc_client


    async def fetch_reminders(self):
        """Fetch all inactive reminders from the database."""
        reminders = await self.db_client.reminders.find({'type': 'inactivity'}).to_list(length=None)
        return reminders

    async def parse_time(self, reminder_setting):
        if 'time' in reminder_setting:
            # Parse the hours from the format 'XX hr'
            hours = int(reminder_setting['time'].replace(' hr', ''))
            return hours * 3600  # Convert hours to seconds
        return None

    async def fetch_inactive_members(self, inactive_threshold, clan_members):
        """Fetch members who are inactive from the database within a specific hour window."""
        now = pend.now('UTC')
        lower_bound = now.subtract(seconds=inactive_threshold + 3600 - 1)
        upper_bound = now.subtract(seconds=inactive_threshold)

        # Fetch all members from the database asynchronously
        inactive_members = await self.db_client.player_stats.find({
            'tag': {'$in': clan_members},
            'last_online': {'$gte': lower_bound.int_timestamp, '$lt': upper_bound.int_timestamp}
        }).to_list(length=None)

        return inactive_members

    async def send_to_kafka(self, inactive_members, reminder):
        """Send notifications about inactive members."""
        sanitized_reminder = json.loads(json.dumps(reminder, default=serialize))
        for member in inactive_members:
            try:
                message = json.dumps({
                    "name": member["name"],
                    "last_online": member["last_online"],
                    "status": "inactive",
                    "type": "inactivity_notification",
                    "reminder": sanitized_reminder  # Include the sanitized reminder document
                }).encode('utf-8')
                topic = 'member_notifications'
                key = str(member['_id']).encode('utf-8')

                # Calculate for how many time the member has been inactive (timestamp difference)
                inactive_time = pend.now('UTC').int_timestamp - member['last_online']

                await self.kafka_producer.send(topic, key=key, value=message, timestamp_ms=inactive_time * 1000)
                # self.logger.info(f'Sent notification to Kafka for inactive member: {member["name"]}')
            except Exception as e:
                self.logger.error(f'Error sending notification to Kafka: {e}')

    async def process_clan(self, clan_tag, thresholds, reminder):
        """Process a single clan and its thresholds."""
        try:
            clan = await self.config.coc_client.get_clan(clan_tag)
            if clan:
                # self.logger.info(f'Processing clan {clan.name} with thresholds: {thresholds}')
                clan_members = [member.tag for member in clan.members]

                # Check for inactive members per threshold
                for threshold in thresholds:
                    inactive_members = await self.fetch_inactive_members(threshold, clan_members)
                    if inactive_members:
                        await self.send_to_kafka(inactive_members, reminder)
        except Exception as e:
            self.logger.error(f'Error processing clan {clan_tag}: {e}')

    async def run(self):
        """Run the inactivity tracking loop."""
        try:
            self.logger.info('Inactivity tracking: Start tracking.')
            start_time = pend.now('UTC')
            reminders = await self.fetch_reminders()
            clans_to_thresholds = {}

            # Group reminders by clan with associated thresholds
            for reminder in reminders:
                try:
                    threshold = await self.parse_time(reminder)
                    if threshold:
                        clan_tag = reminder['clan']
                        if clan_tag not in clans_to_thresholds:
                            clans_to_thresholds[clan_tag] = []
                        clans_to_thresholds[clan_tag].append((threshold, reminder))
                except Exception as e:
                    self.logger.error(f'Error parsing reminder: {e}')

            # Process each clan concurrently
            tasks = [
                self.process_clan(clan_tag, [t[0] for t in thresholds], thresholds[0][1])
                for clan_tag, thresholds in clans_to_thresholds.items()
            ]
            await asyncio.gather(*tasks)
            total_time = (pend.now('UTC') - start_time).total_seconds()
            self.logger.info('Inactivity tracking: Finished processing in {:.2f} seconds.'.format(total_time))

        except Exception as e:
            self.logger.error(f'General error in inactivity tracking loop: {e}')

async def main():
    tracker = InactivityReminderTracker()
    await tracker.run()

