import asyncio
from typing import Optional

import pendulum as pend
import sentry_sdk
from pymongo import UpdateOne

from scripts.tracking import Tracking


class RosterAutomationTracking(Tracking):
    """
    Schedule and fire roster automations using APScheduler date jobs.

    Startup:
      load_and_schedule_all() — full scan once; adds one APScheduler date job
      per (automation, roster) pair.

    Sync (every 5 minutes — delta only, cheap):
      sync_changes() — queries automations and rosters whose updated_at >
      last_sync. Cancels and reschedules only the affected jobs.
      Handles: create, update, deactivate, and event_start_time changes.

    Deletions (every hour):
      reconcile_jobs() — compares in-memory scheduled jobs against active
      automations in DB; cancels any orphaned jobs.

    Firing:
      _fire_automation() — reads is_recurring from DB at fire time, sends
      Kafka, writes log, marks one-shot automations as executed.

    Maintenance (every hour):
      advance_recurring_rosters() — advances event_start_time for recurring
      rosters. Sets updated_at so sync_changes() picks up the reschedule.
    """

    def __init__(self):
        super().__init__(batch_size=1_000)
        # Maps automation_id → list of APScheduler job IDs (one per roster)
        self._aid_to_jobs: dict[str, list[str]] = {}
        # Timestamp of last sync_changes run; updated each cycle
        self._last_sync: int = 0

    # ─────────────────────────────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _job_id(automation_id: str, roster_id: str) -> str:
        return f"roster_auto_{automation_id}_{roster_id}"

    @staticmethod
    def _get_event_start(roster: dict) -> Optional[int]:
        """Field may be stored as 'time' or 'event_start_time' depending on version."""
        return roster.get("time") or roster.get("event_start_time")

    def _schedule_single(self, automation: dict, roster: dict) -> bool:
        """Add or replace an APScheduler date job for one (automation, roster) pair.
        Returns True if a job was actually scheduled."""
        event_start = self._get_event_start(roster)
        if not event_start:
            return False

        trigger_time = event_start + automation["offset_seconds"]
        now = int(pend.now(tz=pend.UTC).timestamp())
        if trigger_time <= now:
            return False  # Already past

        fire_at = pend.from_timestamp(trigger_time, tz=pend.UTC).to_datetime_string()

        automation_id = automation["automation_id"]
        roster_id = roster["custom_id"]
        jid = self._job_id(automation_id, roster_id)

        try:
            self.scheduler.remove_job(jid)
        except Exception:
            pass

        self.scheduler.add_job(
            self._fire_automation,
            "date",
            run_date=pend.from_timestamp(trigger_time, tz=pend.UTC),
            args=[
                automation_id,
                automation["server_id"],
                automation["action_type"],
                roster_id,
                automation.get("group_id"),
                automation.get("discord_channel_id"),
                automation.get("options", {}),
                event_start,
            ],
            id=jid,
            misfire_grace_time=600,
            max_instances=1,
        )

        self._aid_to_jobs.setdefault(automation_id, [])
        if jid not in self._aid_to_jobs[automation_id]:
            self._aid_to_jobs[automation_id].append(jid)

        self.logger.debug(
            f"Scheduled job {jid} | action={automation['action_type']} "
            f"offset={automation['offset_seconds']}s | fires at {fire_at} UTC"
        )
        return True

    def _cancel_single_job(self, automation_id: str, roster_id: str) -> None:
        """Cancel the APScheduler job for a specific (automation, roster) pair."""
        jid = self._job_id(automation_id, roster_id)
        try:
            self.scheduler.remove_job(jid)
        except Exception:
            pass
        if automation_id in self._aid_to_jobs:
            self._aid_to_jobs[automation_id] = [
                j for j in self._aid_to_jobs[automation_id] if j != jid
            ]

    def _cancel_automation_jobs(self, automation_id: str) -> None:
        """Cancel ALL APScheduler jobs for a given automation (all rosters)."""
        for jid in self._aid_to_jobs.pop(automation_id, []):
            try:
                self.scheduler.remove_job(jid)
            except Exception:
                pass

    async def _get_rosters_for_automation(self, automation: dict) -> list[dict]:
        """Return the roster(s) targeted by an automation."""
        projection = {
            "_id": 0, "custom_id": 1, "group_id": 1,
            "time": 1, "event_start_time": 1,
            "recurrence_days": 1, "recurrence_day_of_month": 1,
        }
        if automation.get("roster_id"):
            r = await self.async_mongo.rosters.find_one(
                {"custom_id": automation["roster_id"]}, projection
            )
            return [r] if r else []
        elif automation.get("group_id"):
            return await self.async_mongo.rosters.find(
                {"group_id": automation["group_id"]}, projection
            ).to_list(length=None)
        return []

    # ─────────────────────────────────────────────────────────────────────
    # STARTUP
    # ─────────────────────────────────────────────────────────────────────

    async def load_and_schedule_all(self) -> None:
        """Full scan at startup — schedules every active and future automation."""
        try:
            automations = await self.async_mongo.roster_automation.find(
                {"active": True, "executed": False},
                {
                    "_id": 0, "automation_id": 1, "server_id": 1, "action_type": 1,
                    "roster_id": 1, "group_id": 1, "offset_seconds": 1,
                    "discord_channel_id": 1, "options": 1,
                },
            ).to_list(length=None)

            scheduled = 0
            missed = 0
            now = int(pend.now(tz=pend.UTC).timestamp())

            for automation in automations:
                rosters = await self._get_rosters_for_automation(automation)
                for roster in rosters:
                    event_start = self._get_event_start(roster)
                    if event_start:
                        trigger_time = event_start + automation["offset_seconds"]
                        if trigger_time <= now:
                            is_recurring = bool(
                                roster.get("recurrence_days") or roster.get("recurrence_day_of_month")
                            )
                            if not is_recurring:
                                await self.async_mongo.roster_automation.update_one(
                                    {"automation_id": automation["automation_id"]},
                                    {"$set": {"executed": True, "executed_at": now, "execution_status": "missed"}},
                                )
                            else:
                                await self.async_mongo.roster_automation.update_one(
                                    {"automation_id": automation["automation_id"]},
                                    {"$set": {"last_missed_at": trigger_time}},
                                )
                            sentry_sdk.capture_message(
                                f"Missed automation {automation['automation_id']} "
                                f"(server {automation['server_id']}, action {automation['action_type']}, "
                                f"recurring={is_recurring})",
                                level="warning",
                            )
                            missed += 1
                            continue
                    if self._schedule_single(automation, roster):
                        scheduled += 1

            self._last_sync = int(pend.now(tz=pend.UTC).timestamp())
            total_jobs = sum(len(v) for v in self._aid_to_jobs.values())
            self.logger.info(
                f"[Startup] {len(automations)} automation(s) loaded | "
                f"{scheduled} job(s) scheduled | {missed} missed | "
                f"{total_jobs} total active jobs"
            )
        except Exception:
            self.logger.exception("Error in load_and_schedule_all")

    # ─────────────────────────────────────────────────────────────────────
    # SYNC (every 5 minutes — delta only)
    # ─────────────────────────────────────────────────────────────────────

    async def sync_changes(self) -> None:
        """
        Poll for automations and rosters updated since last sync.
        Handles creates, updates, deactivations, and event_start_time changes.
        Deletions are handled by reconcile_jobs() (hourly).
        """
        try:
            now = int(pend.now(tz=pend.UTC).timestamp())
            since = pend.from_timestamp(self._last_sync, tz=pend.UTC)
            self._last_sync = now

            # ── 1. Automations changed since last sync ──────────────────
            changed_automations = await self.async_mongo.roster_automation.find(
                {"updated_at": {"$gt": since}},
                {
                    "_id": 0, "automation_id": 1, "server_id": 1, "action_type": 1,
                    "roster_id": 1, "group_id": 1, "offset_seconds": 1,
                    "discord_channel_id": 1, "options": 1,
                    "active": 1, "executed": 1,
                },
            ).to_list(length=None)

            rescheduled = 0
            for automation in changed_automations:
                aid = automation["automation_id"]
                self._cancel_automation_jobs(aid)
                if automation.get("active") and not automation.get("executed"):
                    rosters = await self._get_rosters_for_automation(automation)
                    for roster in rosters:
                        if self._schedule_single(automation, roster):
                            rescheduled += 1

            if changed_automations:
                self.logger.info(
                    f"[Sync] {len(changed_automations)} automation change(s) | "
                    f"{rescheduled} rescheduled"
                )

            # ── 2. Rosters with changed event_start_time since last sync ─
            changed_rosters = await self.async_mongo.rosters.find(
                {"updated_at": {"$gt": since}},
                {
                    "_id": 0, "custom_id": 1, "group_id": 1,
                    "time": 1, "event_start_time": 1,
                    "recurrence_days": 1, "recurrence_day_of_month": 1,
                },
            ).to_list(length=None)

            roster_reschedules = 0
            for roster in changed_rosters:
                roster_id = roster["custom_id"]
                group_id = roster.get("group_id")

                query: dict = {"active": True, "executed": False}
                if group_id:
                    query["$or"] = [
                        {"roster_id": roster_id},
                        {"group_id": group_id},
                    ]
                else:
                    query["roster_id"] = roster_id

                automations = await self.async_mongo.roster_automation.find(
                    query, {"_id": 0}
                ).to_list(length=None)

                for automation in automations:
                    self._cancel_single_job(automation["automation_id"], roster_id)
                    if self._schedule_single(automation, roster):
                        roster_reschedules += 1

            if changed_rosters:
                self.logger.info(
                    f"[Sync] {len(changed_rosters)} roster change(s) | "
                    f"{roster_reschedules} job(s) rescheduled"
                )

        except Exception:
            self.logger.exception("Error in sync_changes")

    # ─────────────────────────────────────────────────────────────────────
    # RECONCILE (every hour — handles deletions)
    # ─────────────────────────────────────────────────────────────────────

    async def reconcile_jobs(self) -> None:
        """
        Compare in-memory scheduled jobs against active automations in DB.
        Cancels orphaned jobs left by deletions (which have no updated_at).
        """
        try:
            active_aids = set()
            async for doc in self.async_mongo.roster_automation.find(
                {"active": True, "executed": False},
                {"_id": 0, "automation_id": 1},
            ):
                active_aids.add(doc["automation_id"])

            orphaned = [aid for aid in list(self._aid_to_jobs) if aid not in active_aids]
            for aid in orphaned:
                self._cancel_automation_jobs(aid)

            if orphaned:
                self.logger.info(f"Reconciled {len(orphaned)} orphaned automation job(s)")

        except Exception:
            self.logger.exception("Error in reconcile_jobs")

    # ─────────────────────────────────────────────────────────────────────
    # FIRE
    # ─────────────────────────────────────────────────────────────────────

    async def _fire_automation(
        self,
        automation_id: str,
        server_id: int,
        action_type: str,
        roster_id: str,
        group_id: Optional[str],
        discord_channel_id: Optional[str],
        options: dict,
        event_start_time: int,
    ) -> None:
        """Called by APScheduler at the scheduled trigger time."""
        try:
            now = int(pend.now(tz=pend.UTC).timestamp())

            # Verify automation is still active before firing
            automation = await self.async_mongo.roster_automation.find_one(
                {"automation_id": automation_id, "active": True, "executed": False},
                {"_id": 0},
            )
            if not automation:
                self.logger.warning(
                    f"[Fire] Automation {automation_id} no longer active/valid at fire time — skipped"
                )
                return

            # Read is_recurring from DB at fire time to reflect any changes since scheduling
            roster = await self.async_mongo.rosters.find_one(
                {"custom_id": roster_id},
                {"_id": 0, "recurrence_days": 1, "recurrence_day_of_month": 1},
            )
            is_recurring = bool(
                roster
                and (roster.get("recurrence_days") or roster.get("recurrence_day_of_month"))
            )

            self._send_to_kafka(
                topic="roster_automation",
                data={
                    "automation_id": automation_id,
                    "server_id": server_id,
                    "action_type": action_type,
                    "roster_id": roster_id,
                    "group_id": group_id,
                    "discord_channel_id": discord_channel_id,
                    "options": options,
                },
                key=str(server_id),
            )

            if not is_recurring:
                await self.async_mongo.roster_automation.update_one(
                    {"automation_id": automation_id},
                    {"$set": {"executed": True, "executed_at": now, "execution_status": "triggered"}},
                )
                self._cancel_automation_jobs(automation_id)
            else:
                # Clear any missed flag from a previous skipped cycle
                await self.async_mongo.roster_automation.update_one(
                    {"automation_id": automation_id},
                    {"$unset": {"last_missed_at": ""}},
                )

            self.logger.info(
                f"[Fire] automation={automation_id} action={action_type} "
                f"roster={roster_id} recurring={is_recurring} server={server_id}"
            )
        except Exception:
            self.logger.exception(
                f"Error firing automation {automation_id} for roster {roster_id}"
            )

    # ─────────────────────────────────────────────────────────────────────
    # RECURRING EVENT ADVANCE
    # ─────────────────────────────────────────────────────────────────────

    async def advance_recurring_rosters(self) -> None:
        """
        Advance event_start_time for recurring rosters past their event (+ 1 h grace).
        Sets updated_at so sync_changes() detects the new event time and reschedules
        the affected automation jobs automatically.
        """
        try:
            now = int(pend.now(tz=pend.UTC).timestamp())
            grace = 3_600

            rosters = await self.async_mongo.rosters.find(
                {
                    "$and": [
                        {"$or": [
                            {"time": {"$lt": now - grace}},
                            {"event_start_time": {"$lt": now - grace}},
                        ]},
                        {"$or": [
                            {"recurrence_days": {"$gt": 0}},
                            {"recurrence_day_of_month": {"$gt": 0}},
                        ]},
                    ],
                },
                {
                    "_id": 0, "custom_id": 1,
                    "time": 1, "event_start_time": 1,
                    "recurrence_days": 1, "recurrence_day_of_month": 1,
                },
            ).to_list(length=None)

            if not rosters:
                return

            updates: list[UpdateOne] = []
            for roster in rosters:
                event_start = self._get_event_start(roster)
                if not event_start:
                    continue

                recurrence_days = roster.get("recurrence_days")
                recurrence_dom = roster.get("recurrence_day_of_month")

                try:
                    if recurrence_days:
                        period = recurrence_days * 86_400
                        next_event = event_start + period
                        while next_event + grace < now:
                            next_event += period

                    elif recurrence_dom:
                        dt = pend.from_timestamp(event_start, tz=pend.UTC)
                        next_dt = dt.add(months=1)
                        next_dt = next_dt.set(day=min(recurrence_dom, next_dt.days_in_month))
                        next_event = int(next_dt.timestamp())
                        while next_event + grace < now:
                            next_dt = next_dt.add(months=1)
                            next_dt = next_dt.set(day=min(recurrence_dom, next_dt.days_in_month))
                            next_event = int(next_dt.timestamp())
                    else:
                        continue

                except Exception:
                    self.logger.exception(
                        f"Error computing next event for roster {roster['custom_id']}"
                    )
                    continue

                # updated_at is set so sync_changes() reschedules automation jobs
                updates.append(UpdateOne(
                    {"custom_id": roster["custom_id"]},
                    {"$set": {
                        "time": next_event,
                        "event_start_time": next_event,
                        "updated_at": pend.now(tz=pend.UTC),
                    }},
                ))

            if updates:
                await self.async_mongo.rosters.bulk_write(updates, ordered=False)
                self.logger.info(
                    f"[Advance] Advanced event_start_time for {len(updates)}/{len(rosters)} recurring roster(s)"
                )

        except Exception:
            self.logger.exception("Unexpected error in advance_recurring_rosters")

    # ─────────────────────────────────────────────────────────────────────
    # RUN
    # ─────────────────────────────────────────────────────────────────────

    async def run(self) -> None:
        await self.initialize()
        await self.load_and_schedule_all()

        self.scheduler.add_job(
            self.sync_changes,
            "interval",
            minutes=5,
            name="Sync Automation Changes",
            misfire_grace_time=60,
            max_instances=1,
        )
        self.scheduler.add_job(
            self.advance_recurring_rosters,
            "interval",
            hours=1,
            name="Advance Recurring Rosters",
            misfire_grace_time=600,
            max_instances=1,
        )
        self.scheduler.add_job(
            self.reconcile_jobs,
            "interval",
            hours=1,
            name="Reconcile Orphaned Jobs",
            misfire_grace_time=600,
            max_instances=1,
        )
        self.scheduler.start()

        self.logger.info(
            "RosterAutomationTracking started — sync=5min, advance=1h, reconcile=1h"
        )

        while True:
            await asyncio.sleep(3600)
            self._submit_stats()
