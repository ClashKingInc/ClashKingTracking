import asyncio
import json

class MockKafkaProducer:
    """Mock KafkaProducer for testing purposes with async methods."""

    def __init__(self):
        self.messages = []  # Store all messages sent

    async def send(self, topic, value, key, timestamp_ms=None):
        """Asynchronously mock sending a message to Kafka."""
        # Simulate network delay
        await asyncio.sleep(0.1)  # Sleep to simulate async behavior

        # Decode the 'value' field
        decoded_value = value.decode('utf-8')
        try:
            parsed_value = json.loads(decoded_value)  # Try to parse as JSON
        except json.JSONDecodeError:
            parsed_value = decoded_value  # If not JSON, keep as raw string

        # Add full message details to the messages list
        self.messages.append(
            {
                'topic': topic,
                'key': key.decode('utf-8'),
                'value': parsed_value,  # Store the full parsed value
                'timestamp_ms': timestamp_ms,
            }
        )

        # Print the full message details
        print(f'[MOCK PRODUCER] Message sent: {self.messages[-1]}')

    async def stop(self):
        """Asynchronously mock stopping the producer."""
        await asyncio.sleep(0.1)  # Sleep to simulate async behavior
        print('[MOCK PRODUCER] Stopped Kafka producer.')
