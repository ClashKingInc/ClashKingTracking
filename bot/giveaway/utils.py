import orjson
from loguru import logger


# Kafka event producer
async def produce_giveaway_event(producer, event_type, giveaway):
    """
    Send a Kafka event for the given giveaway.
    """
    # Build the base message
    message = {
        'type': event_type,
        'giveaway': giveaway,
    }

    logger.info(f'Sending Kafka event: {message}')
    # Send the message to Kafka
    await producer.send_and_wait(
        topic='giveaway',
        value=orjson.dumps(message),
    )
