import pendulum as pend


def is_raids():
    """
    Check if the current time is within the raid tracking window (Friday 7:00 UTC to Monday 9:00 UTC).
    """
    now = pend.now('UTC')
    friday_7am = now.start_of('week').add(days=4, hours=7)  # Friday 7:00 UTC
    monday_9am = now.start_of('week').add(days=7, hours=9)  # Monday 9:00 UTC
    return friday_7am <= now < monday_9am
