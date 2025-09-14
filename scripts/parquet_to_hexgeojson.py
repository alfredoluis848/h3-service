#!/usr/bin/env python
import argparse, os, json, h3, pandas as pd

def cell_boundary(cell):
    if hasattr(h3, "cell_to_boundary"):
        coords = h3.cell_to_boundary(cell)    # [(lat,lng), ...]
    else:
        coords = h3.h3_to_geo_boundary(cell)
    ring = [[float(lng), float(lat)] for (lat, lng) in coords]
    ring.append(ring[0])  # fechar
    return {"type": "Polygon", "coordinates": [ring]}

def main():
    ap = argparse.ArgumentParser(description="Parquet (cell_h3, ndvi_mean) -> GeoJSON de hexágonos")
    ap.add_argument("--in", dest="in_parquet", required=True)
    ap.add_argument("--out", dest="out_geojson", required=True)
    ap.add_argument("--ndvi-col", default="ndvi_mean")
    args = ap.parse_args()

    df = pd.read_parquet(args.in_parquet)
    if "cell_h3" not in df.columns or args.ndvi_col not in df.columns:
        raise SystemExit("Parquet precisa ter colunas: cell_h3 e ndvi_mean (ou --ndvi-col).")

    feats = []
    for cell, ndvi in zip(df["cell_h3"], df[args.ndvi_col]):
        geom = cell_boundary(cell)
        feats.append({"type":"Feature","properties":{"cell_h3":cell,"ndvi_mean":float(ndvi)},"geometry":geom})

    fc = {"type":"FeatureCollection","features":feats}
    os.makedirs(os.path.dirname(args.out_geojson) or ".", exist_ok=True)
    with open(args.out_geojson, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False)
    print(f"GeoJSON salvo: {args.out_geojson} ({len(feats)} hex)")

if __name__ == "__main__":
    main()
