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
        "giveaway_id": giveaway['_id'],
        "server_id": giveaway['server_id'],
        "channel_id": giveaway['channel_id'],
        "prize": giveaway['prize'],
        "mentions": giveaway.get('mentions', []),
        "winner_count": giveaway.get('winners'),
        "text_above_embed": giveaway.get('text_above_embed', "") if event_type == "giveaway_start" else None,
        "text_in_embed": giveaway.get('text_in_embed', "") if event_type == "giveaway_start" else None,
        "text_on_end": giveaway.get('text_on_end', "") if event_type == "giveaway_end" else None,
        "image_url": giveaway.get('image_url', None),
    }

    if event_type == "giveaway_start":
        end_time = giveaway.get('end_time')
        message["end_time"] = end_time.timestamp()

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
