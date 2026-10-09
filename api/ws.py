import asyncio
import json
import uuid

from confluent_kafka import Consumer
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

import config

router = APIRouter()

PUSH_INTERVAL_SECONDS = 5


def make_consumer(topic):
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": f"ws-{topic}-{uuid.uuid4()}",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([topic])
    return consumer


@router.websocket("/ws/live-map")
async def live_map(ws: WebSocket):
    await ws.accept()
    consumer = make_consumer("taxi-locations")
    latest = {}
    loop = asyncio.get_event_loop()
    try:
        while True:
            deadline = loop.time() + PUSH_INTERVAL_SECONDS
            while loop.time() < deadline:
                msg = await loop.run_in_executor(None, consumer.poll, 0.5)
                if msg is None or msg.error():
                    continue
                data = json.loads(msg.value())
                taxi_id = data["taxi_id"]
                if data.get("type") == "END":
                    latest.pop(taxi_id, None)
                else:
                    latest[taxi_id] = data
            await ws.send_json(list(latest.values()))
    except WebSocketDisconnect:
        pass
    finally:
        consumer.close()


@router.websocket("/ws/incidents")
async def incidents(ws: WebSocket):
    await ws.accept()
    consumer = make_consumer("violation-events")
    loop = asyncio.get_event_loop()
    try:
        while True:
            msg = await loop.run_in_executor(None, consumer.poll, 1.0)
            if msg is None or msg.error():
                continue
            await ws.send_text(msg.value().decode("utf-8"))
    except WebSocketDisconnect:
        pass
    finally:
        consumer.close()


@router.websocket("/ws/fleet-status")
async def fleet_status(ws: WebSocket):
    await ws.accept()
    consumer = make_consumer("fleet-metrics")
    loop = asyncio.get_event_loop()
    try:
        while True:
            deadline = loop.time() + PUSH_INTERVAL_SECONDS
            latest = None
            while loop.time() < deadline:
                msg = await loop.run_in_executor(None, consumer.poll, 0.5)
                if msg is None or msg.error():
                    continue
                latest = msg.value().decode("utf-8")
            if latest is not None:
                await ws.send_text(latest)
    except WebSocketDisconnect:
        pass
    finally:
        consumer.close()
