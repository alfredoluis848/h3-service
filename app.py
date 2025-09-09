# app.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, conint
from typing import List, Tuple, Literal, Iterable, Set
import math
import h3

app = FastAPI(title="H3 Service", version="0.3.0")

# ---------- helpers: compat v3/v4 ----------
def _latlng_to_cell(lat: float, lng: float, res: int) -> str:
    if hasattr(h3, "latlng_to_cell"):      # v4
        return h3.latlng_to_cell(lat, lng, res)
    if hasattr(h3, "geo_to_h3"):           # v3
        return h3.geo_to_h3(lat, lng, res)
    raise RuntimeError("H3: função de index não encontrada (v3/v4).")

def _cell_to_boundary(cell: str) -> List[Tuple[float, float]]:
    if hasattr(h3, "cell_to_boundary"):    # v4
        return h3.cell_to_boundary(cell)
    if hasattr(h3, "h3_to_geo_boundary"):  # v3
        return h3.h3_to_geo_boundary(cell)
    raise RuntimeError("H3: função de boundary não encontrada (v3/v4).")

def _grid_disk(cell: str, k: int) -> List[str]:
    if hasattr(h3, "grid_disk"):           # v4
        return list(h3.grid_disk(cell, k))
    if hasattr(h3, "k_ring"):              # v3
        return list(h3.k_ring(cell, k))
    raise RuntimeError("H3: função de vizinhança não encontrada (v3/v4).")

# --------- modelos p/ entrada Polyfill (GeoJSON) ----------
class GeoJSONPolygon(BaseModel):
    type: Literal["Polygon"] = "Polygon"
    # GeoJSON usa [lng, lat]
    coordinates: List[List[List[float]]] = Field(
        ..., description="[[[lng,lat], [lng,lat], ...]] com 1 anel obrigatório"
    )

class PolyfillRequest(BaseModel):
    polygon: GeoJSONPolygon
    res: conint(ge=0, le=15) = 9

# ---------------- endpoints básicos ----------------
@app.get("/healthz")
def health():
    return {
        "status": "ok",
        "h3_version": getattr(h3, "__version__", "unknown"),
        "has_v4": hasattr(h3, "latlng_to_cell") and hasattr(h3, "cell_to_boundary"),
    }

@app.get("/h3/index")
def latlng_to_h3(lat: float, lng: float, res: int = 9):
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0):
        raise HTTPException(400, "lat/lng fora dos limites")
    if res < 0 or res > 15:
        raise HTTPException(400, "res must be between 0 and 15")
    try:
        return {"cell": _latlng_to_cell(lat, lng, res)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/h3/boundary/{cell}")
def cell_boundary(cell: str):
    try:
        return {"boundary": _cell_to_boundary(cell)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/h3/kring")
def kring(cell: str, k: conint(ge=0, le=10) = 1):
    try:
        neighbors = _grid_disk(cell, k)
        return {"cells": neighbors}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ---------- util: normalizações e PNP ----------
def _normalize_ring_lnglat(ring: List[List[float]]) -> List[List[float]]:
    # Remove duplicação de primeiro/último ponto se houver; manteremos aberto internamente
    if len(ring) >= 2 and ring[0] == ring[-1]:
        return ring[:-1]
    return ring

def _bbox(ring: List[List[float]]) -> Tuple[float, float, float, float]:
    lngs = [p[0] for p in ring]
    lats = [p[1] for p in ring]
    return min(lngs), min(lats), max(lngs), max(lats)

def _point_in_poly(lng: float, lat: float, ring: List[List[float]]) -> bool:
    # Ray casting (lng,lat) com anel aberto
    inside = False
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % n]
        # checa se cruza a aresta
        cond = ((y1 > lat) != (y2 > lat))
        if cond:
            xin = (x2 - x1) * (lat - y1) / (y2 - y1 + 1e-15) + x1
            if xin > lng:
                inside = not inside
    return inside

def _estimate_deg_step(res: int, lat_mid: float, max_pts: int, w: float, h: float) -> Tuple[float, float, int, int]:
    """
    Define passos de amostragem em graus para manter o total de pontos <= max_pts.
    Começa com estimativa de ~100-400m (0.001..0.004 deg) e ajusta ao bbox.
    """
    # base step ~ 0.002 deg (~200m eq na linha do equador), ajusta com cos(lat)
    base = 0.002
    step_lng = base / max(math.cos(math.radians(lat_mid)), 0.1)
    step_lat = base
    nx = max(1, int(math.ceil(w / step_lng)))
    ny = max(1, int(math.ceil(h / step_lat)))
    # limita total
    scale = math.sqrt((nx * ny) / max_pts) if nx * ny > max_pts else 1.0
    nx = max(1, int(nx / scale))
    ny = max(1, int(ny / scale))
    step_lng = w / nx if nx > 0 else w
    step_lat = h / ny if ny > 0 else h
    return step_lng, step_lat, nx, ny

def _sample_polyfill(ring_lnglat: List[List[float]], res: int, max_pts: int = 2500) -> List[str]:
    """
    Fallback: varre um grid dentro do bbox, filtra por PNP e indexa com H3.
    Retorna lista única de cells.
    """
    ring = _normalize_ring_lnglat(ring_lnglat)
    minx, miny, maxx, maxy = _bbox(ring)
    w, h = maxx - minx, maxy - miny
    if w <= 0 or h <= 0:
        # Polígono degenerado -> usa o primeiro ponto
        lat, lng = ring[0][1], ring[0][0]
        return [ _latlng_to_cell(lat, lng, res) ]

    lat_mid = (miny + maxy) / 2.0
    step_lng, step_lat, nx, ny = _estimate_deg_step(res, lat_mid, max_pts, w, h)

    cells: Set[str] = set()
    # Varredura centro-dos-pixels (evita pegar borda)
    for ix in range(nx):
        lng = minx + (ix + 0.5) * step_lng
        for iy in range(ny):
            lat = miny + (iy + 0.5) * step_lat
            if _point_in_poly(lng, lat, ring):
                try:
                    cells.add(_latlng_to_cell(lat, lng, res))
                except Exception:
                    # ignora pontos inválidos
                    pass

    if not cells:
        # último recurso: pega o centróide aproximado (média) e indexa
        cx = sum(p[0] for p in ring) / len(ring)
        cy = sum(p[1] for p in ring) / len(ring)
        cells.add(_latlng_to_cell(cy, cx, res))
    return list(cells)

# ---------- polyfill (v4 -> v3 -> fallback) ----------
def _polyfill_try_v4(geojson_polygon: dict, res: int) -> List[str]:
    # v4: polygon_to_cells com GeoJSON
    return list(h3.polygon_to_cells(geojson_polygon, res))  # type: ignore[attr-defined]

def _polyfill_try_v3(geojson_polygon: dict, res: int) -> List[str]:
    # v3: polyfill com GeoJSON
    return list(h3.polyfill(geojson_polygon, res, geo_json_conformant=True))  # type: ignore[call-arg]

@app.post("/h3/polyfill")
def polyfill(req: PolyfillRequest):
    """
    Body esperado (GeoJSON):
    {
      "polygon": { "type": "Polygon", "coordinates": [[[lng,lat],...]] },
      "res": 9
    }
    """
    rings = req.polygon.coordinates
    if not rings or not rings[0] or len(rings[0]) < 3:
        raise HTTPException(400, "Polígono inválido: mínimo de 3 pontos no anel externo.")

    # anel externo em [lng,lat]
    ring_lnglat = _normalize_ring_lnglat(rings[0])

    # GeoJSON FECHADO para chamadas diretas
    gj = {"type": "Polygon", "coordinates": [ring_lnglat + [ring_lnglat[0]]]}

    # 1) Tenta v4 -> 2) v3 -> 3) fallback amostragem
    try:
        if hasattr(h3, "polygon_to_cells"):
            cells = _polyfill_try_v4(gj, req.res)
        elif hasattr(h3, "polyfill"):
            cells = _polyfill_try_v3(gj, req.res)
        else:
            raise RuntimeError("Bindings H3 sem polygon_to_cells/polyfill.")
    except Exception:
        # fallback robusto
        cells = _sample_polyfill(ring_lnglat, req.res)

    return {"cells": cells, "count": len(cells)}





