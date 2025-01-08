import random

import pendulum as pend


def generate_custom_id(input_number):
    base_number = (
        input_number
        + int(pend.now(tz=pend.UTC).timestamp())
        + random.randint(1000000000, 9999999999)  # Use random.randint
    )
    return base_number
