# Kafka

This module covers all Kafka infrastructure for the project.

## Folder Structure

```
kafka/
├── producer/         # Kafka Producer (streams T-Drive data into taxi-raw-data)
└── topics/           # Topic creation and configuration scripts
```

## Topics

| Topic | Partitions | Description |
|---|---|---|
| `taxi-raw-data` | 3 | Raw GPS events from the T-Drive producer |
| `taxi-locations` | 3 | Enriched, validated location events from Flink |
| `violation-events` | 3 | Speeding and geofence breach alerts from Flink |
| `fleet-metrics` | 1 | Aggregated fleet-wide statistics from Flink |
| `state-updates` | 3 | Per-taxi state change events from Flink |
