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
            logs_settings = await self._check_war_db_settings(clan_tag)

            for logs_setting in logs_settings:
                await self._send_to_kafka(
                    'war',
                    clan_tag,
                    {
                        'type': 'new_war',
                        'clan_tag': clan_tag,
                        'war': current_war._raw_data,
                        'war_panel_webhook_id': logs_setting[
                            'war_panel_webhook_id'
                        ],
                        'war_log_webhook_id': logs_setting[
                            'war_log_webhook_id'
                        ],
                        'server': logs_setting['server'],
                    },
                )
        else:
            # Detect changes in the war state
            if current_war.state != previous_war.state:
                logs_settings = await self._check_war_db_settings(clan_tag)
                for logs_setting in logs_settings:
                    await self._send_to_kafka(
                        'war',
                        clan_tag,
                        {
                            'type': 'war_state_change',
                            'clan_tag': clan_tag,
                            'old_state': previous_war.state,
                            'new_state': current_war.state,
                            'war_panel_webhook_id': logs_setting[
                                'war_panel_webhook_id'
                            ],
                            'war_log_webhook_id': logs_setting[
                                'war_log_webhook_id'
                            ],
                            'server': logs_setting['server'],
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
        # Extract tags for clarity in logs and debugging
        current_member_tags = [member.tag for member in war.clan.members]
        previous_member_tags = [
            member.tag for member in previous_war.clan.members
        ]

        # Detect added members
        added_members = [
            member._raw_data
            for member in war.clan.members
            if member.tag not in previous_member_tags
        ]

        # Detect removed members
        removed_members = [
            member._raw_data
            for member in previous_war.clan.members
            if member.tag not in current_member_tags
        ]

        if added_members or removed_members:
            logs_settings = await self._check_cwl_db_settings(war.clan.tag)
            for logs_setting in logs_settings:
                await self._send_to_kafka(
                    'cwl_changes',
                    war.clan.tag,
                    {
                        'type': 'cwl_lineup_change',
                        'added': added_members,
                        'removed': removed_members,
                        'war': war._raw_data,
                        'cwl_lineup_change_webhook_id': logs_setting[
                            'cwl_lineup_change_webhook_id'
                        ],
                        'server': logs_setting['server'],
                    },
                )

    async def _detect_member_contributions(
        self, clan_tag, current_war, previous_war
    ):
        """Detect member contributions in the war and send updates to Kafka."""
        try:
            if current_war.attacks:
                clan_member_tags = [
                    member.tag for member in current_war.clan.members
                ]
                # Compare attacks to find new ones
                new_attacks = [
                    attack
                    for attack in current_war.attacks
                    if previous_war is None
                    or attack not in previous_war.attacks
                ]

                if new_attacks:
                    logs_settings = await self._check_war_db_settings(clan_tag)
                    clan_member_tags = [
                        member.tag for member in current_war.clan.members
                    ]

                    # Annotate each attack with its attack_type
                    for attack in new_attacks:
                        if attack.attacker_tag in clan_member_tags:
                            attack._raw_data['attack_type'] = 'offense'
                        elif attack.defender_tag in clan_member_tags:
                            attack._raw_data['attack_type'] = 'defense'

                        # Find the best previous attack on the same defender
                        best_previous_attack = None

                        # Look for the best previous attack in the clan
                        for member in previous_war.clan.members:
                            if member.tag == attack.defender_tag:
                                best_previous_attack = member._raw_data.get(
                                    'bestOpponentAttack'
                                )
                                break

                        # If not found, look for the best previous attack in the opponent clan
                        if not best_previous_attack:
                            for member in previous_war.opponent.members:
                                if member.tag == attack.defender_tag:
                                    best_previous_attack = (
                                        member._raw_data.get(
                                            'bestOpponentAttack'
                                        )
                                    )
                                    break

                        attack._raw_data['best_previous_attack'] = (
                            best_previous_attack
                            if best_previous_attack
                            else None
                        )

                    # Send new attacks to Kafka
                    for logs_setting in logs_settings:
                        await self._send_to_kafka(
                            'war',
                            clan_tag,
                            {
                                'type': 'new_attacks',
                                'clan_tag': clan_tag,
                                'attacks': [
                                    attack._raw_data for attack in new_attacks
                                ],
                                'war_panel_webhook_id': logs_setting[
                                    'war_panel_webhook_id'
                                ],
                                'war_log_webhook_id': logs_setting[
                                    'war_log_webhook_id'
                                ],
                                'server': logs_setting['server'],
                            },
                        )
        except Exception as e:
            self._handle_exception(
                f'Error detecting member contributions for clan {clan_tag}', e
            )

    async def _check_war_db_settings(self, clan_tag):
        """Check the war database settings."""
        result = await self.db_client.clans_db.aggregate(
            [
                {
                    '$match': {
                        'tag': clan_tag,
                        'server': {'$exists': True},
                        'logs': {'$exists': True},
                    }
                },
                {
                    '$project': {
                        '_id': 0,  # Exclude the default MongoDB `_id` field
                        'server': '$server',
                        # Include war_panel_webhook only if it exists
                        'war_panel_webhook_id': {
                            '$cond': {
                                'if': {
                                    '$and': [
                                        {'$ne': ['$logs.war_panel', None]},
                                        {
                                            '$ne': [
                                                '$logs.war_panel.webhook',
                                                None,
                                            ]
                                        },
                                    ]
                                },
                                'then': '$logs.war_panel.webhook',
                                'else': None,
                            }
                        },
                        # Include war_log_webhook only if it exists
                        'war_log_webhook_id': {
                            '$cond': {
                                'if': {
                                    '$and': [
                                        {'$ne': ['$logs.war_log', None]},
                                        {
                                            '$ne': [
                                                '$logs.war_log.webhook',
                                                None,
                                            ]
                                        },
                                    ]
                                },
                                'then': '$logs.war_log.webhook',
                                'else': None,
                            }
                        },
                    }
                },
                {
                    '$match': {
                        # Ensure at least one webhook exists
                        '$or': [
                            {'war_panel_webhook_id': {'$ne': None}},
                            {'war_log_webhook_id': {'$ne': None}},
                        ]
                    }
                },
            ]
        ).to_list(length=None)
        return result

    async def _check_cwl_db_settings(self, clan_tag):
        """Check the CWL database settings."""
        result = await self.db_client.clans_db.aggregate(
            [
                {
                    '$match': {
                        'tag': clan_tag,
                        'server': {'$exists': True},
                        'logs': {'$exists': True},
                        'logs.cwl_lineup_change': {'$exists': True},
                        'logs.cwl_lineup_change.webhook': {
                            '$exists': True,
                            '$ne': None,
                        },
                    }
                },
                {
                    '$project': {
                        '_id': 0,  # Exclude the default MongoDB `_id` field
                        'server': '$server',
                        'cwl_lineup_change_webhook_id': '$logs.cwl_lineup_change.webhook',
                    }
                },
            ]
        ).to_list(length=None)
        return result


if __name__ == '__main__':
    # Create an instance of WarTracker and start the tracking loop
    tracker = WarTracker(tracker_type=TrackingType.BOT_WAR)
    asyncio.run(
        tracker.run(
            tracker_class=WarTracker,
            loop_interval=20,
        )
    )
