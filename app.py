from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, conint
from typing import List, Tuple, Literal, Union, Optional
import h3

app = FastAPI(title="H3 Service", version="0.2.0")

# ---------- helpers: compat entre v3 e v4 ----------
def _latlng_to_cell(lat: float, lng: float, res: int) -> str:
    if hasattr(h3, "latlng_to_cell"):      # v4
        return h3.latlng_to_cell(lat, lng, res)
    if hasattr(h3, "geo_to_h3"):           # v3
        return h3.geo_to_h3(lat, lng, res)
    raise RuntimeError("H3 bindings: função de index não encontrada.")

def _cell_to_boundary(cell: str) -> List[Tuple[float, float]]:
    if hasattr(h3, "cell_to_boundary"):    # v4
        return h3.cell_to_boundary(cell)
    if hasattr(h3, "h3_to_geo_boundary"):  # v3
        return h3.h3_to_geo_boundary(cell)
    raise RuntimeError("H3 bindings: função de boundary não encontrada.")

def _grid_disk(cell: str, k: int) -> List[str]:
    if hasattr(h3, "grid_disk"):           # v4
        return list(h3.grid_disk(cell, k))
    if hasattr(h3, "k_ring"):              # v3
        return list(h3.k_ring(cell, k))
    raise RuntimeError("H3 bindings: função de vizinhança não encontrada.")

def _polygon_to_cells(boundary_latlng: List[Tuple[float, float]], res: int) -> List[str]:
    """
    Recebe um polígono como [(lat,lng), ...] e devolve as células.
    v4: h3.polygon_to_cells
    v3: h3.polyfill
    """
    if hasattr(h3, "polygon_to_cells"):    # v4
        return list(h3.polygon_to_cells(boundary_latlng, res))
    if hasattr(h3, "polyfill"):            # v3
        return list(h3.polyfill(boundary_latlng, res))
    raise RuntimeError("H3 bindings: função de polyfill não encontrada.")

# --------- modelos p/ entrada Polyfill (GeoJSON) ----------
class GeoJSONPolygon(BaseModel):
    type: Literal["Polygon"] = "Polygon"
    # GeoJSON usa [lng, lat]; aqui tipamos genericamente
    coordinates: List[List[List[float]]] = Field(
        ..., description="[[[lng,lat], [lng,lat], ...]] com 1 anel obrigatório"
    )

class PolyfillRequest(BaseModel):
    polygon: GeoJSONPolygon
    res: conint(ge=0, le=15) = 9

# ---------------- endpoints existentes ----------------
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

# ---------------- novos endpoints ----------------

@app.get("/h3/kring")
def kring(cell: str, k: conint(ge=0, le=10) = 1):
    """
    Vizinho até k anéis.
    v4: grid_disk, v3: k_ring
    """
    try:
        neighbors = _grid_disk(cell, k)
        return {"cells": neighbors}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/h3/polyfill")
def polyfill(req: PolyfillRequest):
    """
    Recebe GeoJSON Polygon:
    {
      "polygon": { "type": "Polygon", "coordinates": [[[lng,lat],...]] },
      "res": 9
    }
    """
    try:
        rings = req.polygon.coordinates
        if not rings or not rings[0] or len(rings[0]) < 3:
            raise HTTPException(400, "Polígono inválido: mínimo de 3 pontos no anel externo.")

        # GeoJSON é [lng,lat]; H3 em Python usa [(lat,lng)...]
        exterior_geojson = rings[0]
        boundary_latlng = [(lat, lng) for (lng, lat) in exterior_geojson]

        cells = _polygon_to_cells(boundary_latlng, req.res)
        return {"cells": cells, "count": len(cells)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))




