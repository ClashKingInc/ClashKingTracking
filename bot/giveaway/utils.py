import orjson
from loguru import logger

# Kafka event producer
async def produce_giveaway_event(producer, event_type, giveaway):
    """
    Send a Kafka event for the given giveaway.
    """
    # Build the base message
    message = {
        "type": event_type,
        "channel_id": giveaway['channel_id'],
        "prize": giveaway['prize'],
        "mentions": giveaway.get('mentions', []),
        "winner_count": giveaway.get('winners') if event_type == "giveaway_end" else None,
    }

    # Include participants if it's a giveaway end event
    if event_type == "giveaway_end":
        participants = giveaway.get('entries', [])
        message["participants"] = participants  # Add the list of participants to the message

    logger.info(f"Sending Kafka event: {message}")
    # Send the message to Kafka
    await producer.send_and_wait(
        topic="giveaway",
        value=orjson.dumps(message),
    )
