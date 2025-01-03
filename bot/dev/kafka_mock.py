import json


class MockKafkaProducer:
    """Mock KafkaProducer for testing purposes."""

    def __init__(self):
        self.messages = []  # Store all messages sent

    def send(self, topic, value, key, timestamp_ms):
        # Parsing the 'value' field to extract 'type'
        decoded_value = value.decode('utf-8')
        try:
            parsed_value = json.loads(decoded_value)
            message_type = parsed_value.get('type', 'unknown')
        except json.JSONDecodeError:
            message_type = 'invalid_json'

        self.messages.append(
            {
                'topic': topic,
                'key': key.decode('utf-8'),
                'type': message_type,
                'timestamp_ms': timestamp_ms,
            }
        )
        print(f'[MOCK PRODUCER] Message sent: {self.messages[-1]}')
