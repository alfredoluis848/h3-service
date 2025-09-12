#!/usr/bin/env python
# Ingestão vetorial H3 

import argparse, os
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString, MultiLineString, Polygon, MultiPolygon
import h3

def to_cell(lat, lng, res):
    return h3.latlng_to_cell(lat, lng, res) if hasattr(h3, "latlng_to_cell") else h3.geo_to_h3(lat, lng, res)

def polyfill_polygon(poly: Polygon, res: int):
    
      
   
    try:
        if hasattr(h3, "polyfill"):
            ext = [[float(x), float(y)] for x, y in poly.exterior.coords]
            holes = [[[float(x), float(y)] for x, y in ring.coords] for ring in poly.interiors]
            gj = {"type": "Polygon", "coordinates": [ext] + holes}
            return list(h3.polyfill(gj, res, geo_json_conformant=True))
    except Exception:
        pass

    # 2) Fallback: amostragem regular (aproximado)
    from shapely.geometry import Point as _Point
    import numpy as np
    step_deg = 0.0007  # ~78 m; aumente p/ mais rápido, diminua p/ mais precisão
    minx, miny, maxx, maxy = poly.bounds
    xs = np.arange(minx, maxx + step_deg, step_deg)
    ys = np.arange(miny, maxy + step_deg, step_deg)
    cells = set()
    for x in xs:
        for y in ys:
            p = _Point(x, y)
            if poly.contains(p) or poly.touches(p):
                cells.add(to_cell(y, x, res))
    return list(cells)

def line_to_cells(line: LineString, res: int, step_m: float):
    if line.length == 0:
        x, y = line.coords[0]
        return [to_cell(y, x, res)]
    n = max(1, int((line.length * 111_320) / step_m))  # 1º ~ 111.32 km (aprox)
    pts = [line.interpolate(i / n, normalized=True) for i in range(n + 1)]
    return list({to_cell(p.y, p.x, res) for p in pts})

def geom_to_cells(geom, res: int, step_m: float):
    if geom.is_empty: return []
    t = geom.geom_type
    if t == "Point":
        return [to_cell(geom.y, geom.x, res)]
    if t == "LineString":
        return line_to_cells(geom, res, step_m)
    if t == "Polygon":
        return polyfill_polygon(geom, res)
    if t == "MultiLineString":
        s = set()
        for g in geom.geoms: s.update(line_to_cells(g, res, step_m))
        return list(s)
    if t == "MultiPolygon":
        s = set()
        for g in geom.geoms: s.update(polyfill_polygon(g, res))
        return list(s)
    if t == "MultiPoint":
        return list({to_cell(p.y, p.x, res) for p in geom.geoms})
    return []

def main():
    ap = argparse.ArgumentParser(description="Ingest vetorial -> H3 (sem polygon_to_cells v4)")
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_parquet", default="data/vector_h3.parquet")
    ap.add_argument("--res", type=int, default=9)
    ap.add_argument("--line-step-meters", dest="line_step_m", type=float, default=50.0)
    ap.add_argument("--max-features", type=int, default=None)
    args = ap.parse_args()

    gdf = gpd.read_file(args.in_path)
    gdf = gdf.set_crs(4326, allow_override=True) if gdf.crs is None else gdf.to_crs(4326)
    if args.max_features: gdf = gdf.head(args.max_features)

    rows = []
    attrs = [c for c in gdf.columns if c != "geometry"]
    for _, r in gdf.iterrows():
        cells = geom_to_cells(r.geometry, args.res, args.line_step_m)
        if not cells: continue
        props = {k: (None if pd.isna(r[k]) else r[k]) for k in attrs}
        for c in cells: rows.append({"cell_h3": c, **props})

    if not rows:
        raise SystemExit("Nenhuma célula gerada.")
    os.makedirs(os.path.dirname(args.out_parquet) or ".", exist_ok=True)
    df = pd.DataFrame(rows)
    cols = ["cell_h3"] + [c for c in df.columns if c != "cell_h3"]
    print(f"Gerando {args.out_parquet} com {len(df)} linhas...")
    df[cols].to_parquet(args.out_parquet, index=False)
    print("Concluído.")

if __name__ == "__main__":
    main()
