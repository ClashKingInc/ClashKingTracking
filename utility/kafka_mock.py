class MockKafkaProducer:
    """Mock KafkaProducer for testing purposes."""

    def __init__(self):
        self.messages = []  # Store all messages sent

    def send(self, topic, value, key, timestamp_ms):
        self.messages.append(
            {
                "topic": topic,
                "key": key.decode("utf-8") if key else None,
                "value": value.decode("utf-8") if value else None,
                "timestamp_ms": timestamp_ms,
            }
        )
        # print(f'[MOCK PRODUCER] Message sent: {self.messages[-1]}')
