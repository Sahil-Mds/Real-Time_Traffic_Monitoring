# API Server

This module contains the backend API server exposing:

- **WebSocket APIs** – Real-time push to the dashboard (Live Map, Incidents Feed, Fleet Status)
- **REST APIs** – Query endpoints for the Analytics page

## WebSocket Endpoints

| Endpoint | Feeds | Kafka Source |
|---|---|---|
| `/ws/live-map` | Live taxi positions | `taxi-locations` |
| `/ws/incidents` | Violations & alerts | `violation-events` |
| `/ws/fleet-status` | Fleet-wide stats | `fleet-metrics` |

## REST Endpoints

| Endpoint | Description | Data Source |
|---|---|---|
| `GET /api/taxis` | List all taxis with current state | Redis (Hash) |
| `GET /api/taxis/{id}` | Single taxi detail | Redis (Hash) |
| `GET /api/violations` | Recent violation events | Redis (Set) |
| `GET /api/fleet/stats` | Fleet-wide aggregated metrics | Redis (String) |
