import json

import redis
from fastapi import APIRouter, HTTPException

import config

router = APIRouter(prefix="/api")

r = redis.Redis(
    host=config.REDIS_HOST,
    port=config.REDIS_PORT,
    db=config.REDIS_DB,
    decode_responses=True,
)


@router.get("/fleet/stats")
def fleet_stats():
    value = r.get("fleet:metrics")
    if value is None:
        return {}
    return json.loads(value)


@router.get("/taxis")
def list_taxis():
    taxis = []
    for key in r.scan_iter(match="taxi:*"):
        value = r.get(key)
        if value:
            taxis.append(json.loads(value))
    return taxis


@router.get("/taxis/{taxi_id}")
def get_taxi(taxi_id: int):
    value = r.get(f"taxi:{taxi_id}")
    if value is None:
        raise HTTPException(status_code=404, detail="Taxi not found")
    return json.loads(value)
