import json


def test_health_endpoint(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_fleet_stats_returns_empty_object_when_no_data(client):
    response = client.get("/api/fleet/stats")

    assert response.status_code == 200
    assert response.json() == {}


def test_fleet_stats_returns_metrics_from_redis(client, fake_redis):
    fake_redis.data["fleet:metrics"] = json.dumps({
        "activeTaxis": 12,
        "totalDistanceKm": 345.6,
        "timestamp": "2008-02-02T13:00:00"
    })

    response = client.get("/api/fleet/stats")

    assert response.status_code == 200
    assert response.json() == {
        "activeTaxis": 12,
        "totalDistanceKm": 345.6,
        "timestamp": "2008-02-02T13:00:00"
    }


def test_list_taxis_returns_only_taxi_keys(client, fake_redis):
    fake_redis.data["taxi:101"] = json.dumps({
        "taxiId": 101,
        "avgSpeedKmh": 42.5,
        "totalDistanceKm": 18.3
    })
    fake_redis.data["taxi:202"] = json.dumps({
        "taxiId": 202,
        "avgSpeedKmh": 55.1,
        "totalDistanceKm": 31.9
    })
    fake_redis.data["fleet:metrics"] = json.dumps({
        "activeTaxis": 2
    })

    response = client.get("/api/taxis")

    assert response.status_code == 200

    taxis = response.json()
    taxi_ids = sorted(taxi["taxiId"] for taxi in taxis)

    assert taxi_ids == [101, 202]
    assert len(taxis) == 2


def test_get_taxi_returns_single_taxi(client, fake_redis):
    fake_redis.data["taxi:101"] = json.dumps({
        "taxiId": 101,
        "avgSpeedKmh": 42.5,
        "totalDistanceKm": 18.3
    })

    response = client.get("/api/taxis/101")

    assert response.status_code == 200
    assert response.json() == {
        "taxiId": 101,
        "avgSpeedKmh": 42.5,
        "totalDistanceKm": 18.3
    }


def test_get_taxi_returns_404_when_missing(client):
    response = client.get("/api/taxis/999")

    assert response.status_code == 404
    assert response.json() == {"detail": "Taxi not found"}