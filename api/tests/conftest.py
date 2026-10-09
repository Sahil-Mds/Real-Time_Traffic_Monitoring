import fnmatch
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

API_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(API_DIR))

import main
import rest


class FakeRedis:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def scan_iter(self, match=None):
        for key in self.data.keys():
            if match is None or fnmatch.fnmatch(key, match):
                yield key


@pytest.fixture
def fake_redis(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(rest, "r", fake)
    return fake


@pytest.fixture
def client(fake_redis):
    return TestClient(main.app)