import asyncio
import pendulum as pend
import sentry_sdk
from tracking import Tracking


class ClanTracker(Tracking):
    """Class to manage clan tracking."""

    def __init__(self, tracker_type: str, max_concurrent_requests=1000):
        # Call the parent class constructor
        super().__init__(
            max_concurrent_requests=max_concurrent_requests,
            tracker_type=tracker_type,
        )
        self.clan_cache = {}  # Cache for tracking clan states
        self.last_private_warlog_warn = (
            {}
        )  # Cache for private war log warnings

    async def _track_item(self, clan_tag):
        """Track updates for a specific clan."""
        sentry_sdk.set_context('clan_tracking', {'clan_tag': clan_tag})
        try:
            async with self.semaphore:
                clan = await self.coc_client.get_clan(tag=clan_tag)
        except Exception as e:
            self._handle_exception(f'Error fetching clan {clan_tag}', e)
            return

        previous_clan = self.clan_cache.get(clan.tag)
        self.clan_cache[clan.tag] = clan

        if previous_clan is None:
            return

        sentry_sdk.add_breadcrumb(
            message=f'Tracking clan: {clan_tag}', level='info'
        )
        self._handle_private_warlog(clan)
        self._handle_attribute_changes(clan, previous_clan)
        self._handle_member_changes(clan, previous_clan)
        self._handle_donation_updates(clan, previous_clan)

    def _handle_private_warlog(self, clan):
        """Handle cases where the war log is private."""
        now = pend.now(tz=pend.UTC)
        last_warn = self.last_private_warlog_warn.get(clan.tag)

        if not clan.public_war_log:
            if (
                last_warn is None
                or (now - last_warn).total_seconds() >= 12 * 3600
            ):
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
            attr
            for attr in attributes
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

        members_joined = [
            n._raw_data for n in clan.members if n.tag not in previous_members
        ]
        members_left = [
            m._raw_data
            for m in previous_clan.members
            if m.tag not in current_members
        ]

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
        previous_donations = {
            n.tag: (n.donations, n.received) for n in previous_clan.members
        }

        for member in clan.members:
            if (donated := previous_donations.get(member.tag)) is not None:
                mem_donos, mem_received = donated
                if (
                    mem_donos < member.donations
                    or mem_received < member.received
                ):
                    json_data = {
                        'type': 'all_member_donations',
                        'old_clan': previous_clan._raw_data,
                        'new_clan': clan._raw_data,
                    }
                    self._send_to_kafka('clan', clan.tag, json_data)
                    break


if __name__ == '__main__':
    tracker = ClanTracker(tracker_type='bot_clan')
    asyncio.run(
        tracker.run(
            tracker_class=ClanTracker, config_type='bot_clan', loop_interval=20
        )
    )
