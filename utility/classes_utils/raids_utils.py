import coc
import pendulum as pend
from coc import RaidLogEntry


def is_raids():
    """
    Check if the current time is within the raid tracking window (Friday 7:00 UTC to Monday 9:00 UTC).
    """
    now = pend.now('UTC')
    friday_7am = now.start_of('week').add(days=4, hours=7)  # Friday 7:00 UTC
    monday_9am = now.start_of('week').add(days=7, hours=9)  # Monday 9:00 UTC
    return friday_7am <= now < monday_9am


def get_time_until_next_raid():
    """Calculate the time until the next raid window starts."""
    now = pend.now(tz=pend.UTC)
    next_raid_start = now.start_of('week').add(
        days=4, hours=7
    )  # Friday 7:00 UTC
    if now >= next_raid_start:  # If we're past this week's Friday 7:00 UTC
        next_raid_start = next_raid_start.add(weeks=1)  # Move to next Friday
    sleep_time = (next_raid_start - now).total_seconds()
    return sleep_time


def format_seconds_as_date(seconds):
    """Convert seconds into a datetime string representing the time after the given seconds from now using Pendulum."""
    current_time = pend.now(tz=pend.UTC)
    future_time = current_time.add(seconds=seconds)
    formatted_time = future_time.to_datetime_string()
    return formatted_time


def weekend_to_coc_py_timestamp(weekend: str, end=False) -> coc.Timestamp:
    """
    Convert a weekend date string to a Clash of Clans Timestamp.

    Args:
        weekend (str): The weekend date in 'YYYY-MM-DD' format.
        end (bool, optional): If True, calculate the end timestamp (3 days after the start). Defaults to False.

    Returns:
        coc.Timestamp: A Clash of Clans Timestamp object.
    """
    # Parse the weekend string using Pendulum
    weekend_date = pend.from_format(weekend, 'YYYY-MM-DD', tz='UTC').at(
        7, 0, 0
    )

    # Adjust for end timestamp if specified
    if end:
        weekend_date = weekend_date.add(days=3)

    # Return the formatted Clash of Clans Timestamp
    return coc.Timestamp(data=weekend_date.format('YYYYMMDDTHHmmss.SSS[Z]'))


async def get_raid_log_entry(
    clan: coc.Clan, weekend: str, limit=0, coc_client=None, db_client=None
) -> RaidLogEntry:
    """
    Retrieve a raid log entry for a specific weekend. If the entry is not found in the API,
    attempt to fetch it from the database.

    Args:
        clan (coc.Clan): The clan for which the raid log is being retrieved.
        weekend (str): The weekend identifier.
        limit (int, optional): The limit on the number of raid logs to fetch. Defaults to 0.
        coc_client (coc.Clients, optional): The Clash of Clans API client. Defaults to None.
        db_client (AsyncIOMotorClient, optional): The MongoDB database client. Defaults to None.

    Returns:
        RaidLogEntry or None: The raid log entry if found, otherwise None.
    """
    try:
        # Fetch the raid log from the Clash of Clans API
        raid_log = await coc_client.get_raid_log(
            clan_tag=clan.tag, limit=limit
        )

        # Convert the weekend string to the corresponding Clash of Clans timestamp
        weekend_timestamp = weekend_to_coc_py_timestamp(weekend)

        # Attempt to find the raid log entry for the specified weekend
        weekend_raid: RaidLogEntry = coc.utils.get(
            raid_log, start_time=weekend_timestamp
        )

        # Check if the raid log entry exists and contains any looted resources
        if (
            weekend_raid
            and sum(
                member.capital_resources_looted
                for member in weekend_raid.members
            )
            > 0
        ):
            return weekend_raid

        # If no valid raid log entry is found, check the database
        raid_data = await db_client.raid_weekends.find_one(
            {
                '$and': [
                    {'clan_tag': clan.tag},
                    {
                        'data.startTime': f"{weekend_timestamp.time.strftime('%Y%m%dT%H%M%S.000Z')}"
                    },
                ]
            }
        )

        # If raid data is found in the database, construct a RaidLogEntry object
        if raid_data:
            entry: RaidLogEntry = RaidLogEntry(
                data=raid_data.get('data'),
                client=coc_client,
                clan_tag=clan.tag,
            )
            return entry

        # If no data is found in both the API and the database, return None
        return None
    except Exception as e:
        print(e)
        return None


def calculate_current_weekend():
    """Calculate the current weekend's starting timestamp."""
    return (
        pend.now(tz=pend.UTC)
        .start_of('week')
        .add(days=4)
        .at(7, 0, 0)
        .format('YYYY-MM-DD')
    )
