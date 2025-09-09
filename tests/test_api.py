import pytest
from fastapi.testclient import TestClient
import h3

from app import app

client = TestClient(app)

def test_health():
    r = client.get("/healthz")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert "h3_version" in data

def test_index_and_boundary():
    lat, lng, res = -23.5505, -46.6333, 9
    r = client.get(f"/h3/index?lat={lat}&lng={lng}&res={res}")
    assert r.status_code == 200
    cell = r.json()["cell"]
    assert isinstance(cell, str)

    r2 = client.get(f"/h3/boundary/{cell}")
    assert r2.status_code == 200
    boundary = r2.json()["boundary"]
    assert isinstance(boundary, list)
    assert len(boundary) >= 6

def test_kring():
    lat, lng, res = -23.5505, -46.6333, 9
    cell = client.get(f"/h3/index?lat={lat}&lng={lng}&res={res}").json()["cell"]
    r = client.get(f"/h3/kring?cell={cell}&k=1")
    assert r.status_code == 200
    cells = r.json()["cells"]
    assert len(cells) >= 7  # central + vizinhos

def test_polyfill():
    poly = {
        "polygon": {
            "type": "Polygon",
            "coordinates": [[
                [-46.64, -23.56],
                [-46.62, -23.56],
                [-46.62, -23.54],
                [-46.64, -23.54],
                [-46.64, -23.56]
            ]]
        },
        "res": 9
    }
    r = client.post("/h3/polyfill", json=poly)
    assert r.status_code == 200
    data = r.json()
    assert "cells" in data and data["count"] == len(data["cells"])
