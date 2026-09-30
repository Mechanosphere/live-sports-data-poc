"""Ingestion pipeline: poll provider APIs, normalize payloads, publish to Kafka.

Package layout:
    providers/   Per-provider polling clients that return raw payloads.
    normalize.py Converts each provider's raw payload into our GameEvent schema.
    kafka_producer.py  Publishes normalized events to the football.events topic.
    kafka_consumer.py  Consumes football.events and persists to DynamoDB.
    poller.py    Orchestrates polling + normalization + publishing on an interval.
"""
