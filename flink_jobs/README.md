# Flink Jobs

This module contains all Apache Flink stream processing operators.

## Operators

| Operator | Description |
|---|---|
| `DataCleaner` | Drops nulls, invalid coordinates, and bad timestamps right after ingestion |
| `FilterAndClean` | Deduplicates identical location events to prevent errors |
| `ComputeSpeedAndDistance` | Computes segment distance and speed between consecutive GPS points using Haversine |
| `CalculateAverageSpeed` | Maintains a running average speed per taxi |
| `OverSpeedAlert` | Detects taxis exceeding 50 km/h, writes to `violation-events` |
| `AreaViolationsAlert` | Detects geofence breaches (10km / 15km radius around Forbidden City), writes to `violation-events` |
| `StoreTaxiInfo` | Writes per-taxi state (speed, distance) to Redis and `state-updates` topic |
| `FleetStatsOperator` | Aggregates active taxis and fleet-wide distance, writes to Redis and `fleet-metrics` topic |

## Kafka Topics Consumed / Produced

| Topic | Direction |
|---|---|
| `taxi-raw-data` | Consumed (source) |
| `taxi-locations` | Produced |
| `violation-events` | Produced |
| `fleet-metrics` | Produced |
| `state-updates` | Produced |
