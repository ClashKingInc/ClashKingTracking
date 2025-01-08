import random

import pendulum as pend


def sentry_filter(event, hint):
    """Filter out events that are not errors."""
    if 'exception' in hint:
        exc_type, exc_value, exc_tb = hint['exception']
        if exc_type == KeyboardInterrupt:
            return None
    return event


def generate_custom_id(input_number):
    base_number = (
        input_number
        + int(pend.now(tz=pend.UTC).timestamp())
        + random.randint(1000000000, 9999999999)  # Use random.randint
    )
    return base_number
