from kafka import KafkaProducer


class TestKafkaProducer(KafkaProducer):
    """Custom KafkaProducer for test mode to append '-beta' to topics."""

    def send(
        self,
        topic,
        value=None,
        key=None,
        headers=None,
        partition=None,
        timestamp_ms=None,
    ):
        # Append '-beta' to the topic name
        modified_topic = f'{topic}-beta'
        return super().send(
            modified_topic,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp_ms=timestamp_ms,
        )
