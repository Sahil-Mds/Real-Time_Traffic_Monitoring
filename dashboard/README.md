# Dashboard

This module contains the frontend web UI.

## Pages

| Page | Description | Data Source |
|---|---|---|
| **Live Map** | Full-screen real-time map showing all taxi positions | WebSocket `/ws/live-map` |
| **Incidents Feed** | Live stream of speeding and geofence violations | WebSocket `/ws/incidents` |
| **Fleet Status** | Per-taxi speed, distance, and violation status table | REST `GET /api/taxis` & WebSocket `/ws/fleet-status` |
| **Analytics** | Aggregated statistics, KPIs, and leaderboards | REST `GET /api/taxis` |
