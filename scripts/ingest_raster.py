#!/usr/bin/env python
# Ingestão raster H3 (NDVI) — com reprojeção para EPSG:4326

import argparse, os
import numpy as np
import pandas as pd
import rasterio as rio
import h3

try:
    from pyproj import Transformer  # precisa estar disponível no container ingest
except Exception:
    Transformer = None

def to_cell(lat, lng, res: int) -> str:
    if hasattr(h3, 'latlng_to_cell'):
        return h3.latlng_to_cell(lat, lng, res)
    return h3.geo_to_h3(lat, lng, res)

def main():
    ap = argparse.ArgumentParser(description='Raster -> H3 NDVI')
    ap.add_argument('--in', dest='in_tif', required=True)
    ap.add_argument('--red-band', type=int, default=1)
    ap.add_argument('--nir-band', type=int, default=2)
    ap.add_argument('--res', type=int, default=9)
    ap.add_argument('--step', type=int, default=4, help='amostra a cada N pixels (>=1)')
    ap.add_argument('--max-pixels', type=int, default=400000, help='limite aprox. de pixels processados')
    ap.add_argument('--out', dest='out_parquet', default='data/raster_h3.parquet')
    args = ap.parse_args()

    with rio.open(args.in_tif) as ds:
        # ---- info básica
        crs = ds.crs
        print(f'[ingest] arquivo={args.in_tif}  CRS={crs}')

        # ---- lê bandas como float32
        red = ds.read(args.red_band).astype('float32')
        nir = ds.read(args.nir_band).astype('float32')

        # ---- máscara de validade
        try:
            m1 = ds.read_masks(args.red_band) > 0
            m2 = ds.read_masks(args.nir_band) > 0
            valid = m1 & m2
        except Exception:
            valid = np.isfinite(red) & np.isfinite(nir)

        # ---- NDVI
        ndvi = (nir - red) / (nir + red + 1e-6)
        ndvi[~valid] = np.nan

        H, W = ndvi.shape
        # mantém seu controle de carga
        step_from_cap = max(1, (H * W) // max(1, args.max_pixels))
        step = max(1, max(step_from_cap, args.step))
        print(f'[ingest] shape={H}x{W}  step={step}')

        # ---- transformer para EPSG:4326 (lon/lat)
        dst_epsg = "EPSG:4326"
        if Transformer is not None and crs and crs.to_string() != dst_epsg:
            transformer = Transformer.from_crs(crs, dst_epsg, always_xy=True)
            print(f'[ingest] reprojetando de {crs} -> {dst_epsg}')
        else:
            transformer = None
            print('[ingest] sem reprojeção (já em EPSG:4326 ou pyproj ausente)')

        acc = {}  # cell -> [soma_ndvi, contagem]

        for i in range(0, H, step):
            for j in range(0, W, step):
                v = ndvi[i, j]
                if np.isnan(v):
                    continue

                # coord no CRS do raster
                x, y = ds.xy(i, j)

                # reprojeta para lon/lat se necessário
                if transformer is not None:
                    lon, lat = transformer.transform(x, y)  # always_xy=True: (x=lon/easting, y=lat/northing)
                else:
                    lon, lat = float(x), float(y)  # já é lon/lat

                # guarda no H3
                try:
                    cell = to_cell(lat, lon, args.res)
                except Exception:
                    continue

                s = acc.get(cell)
                if s is None:
                    acc[cell] = [float(v), 1]
                else:
                    s[0] += float(v); s[1] += 1

    if not acc:
        raise SystemExit('Nenhuma célula acumulada (NDVI todo NaN?).')

    rows = [{'cell_h3': c, 'ndvi_mean': s[0] / s[1]} for c, s in acc.items()]
    os.makedirs(os.path.dirname(args.out_parquet) or '.', exist_ok=True)
    pd.DataFrame(rows).to_parquet(args.out_parquet, index=False)
    print(f'[ingest] Gerado {args.out_parquet} com {len(rows)} linhas.')

if __name__ == '__main__':
    main()

