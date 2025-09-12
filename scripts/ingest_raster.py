#!/usr/bin/env python
# Ingestão raster H3 

import argparse, os
import numpy as np
import pandas as pd
import rasterio as rio
import h3

def to_cell(lat, lng, res):
    return h3.latlng_to_cell(lat, lng, res) if hasattr(h3, 'latlng_to_cell') else h3.geo_to_h3(lat, lng, res)

def main():
    ap = argparse.ArgumentParser(description='Raster -> H3 NDVI')
    ap.add_argument('--in', dest='in_tif', required=True)
    ap.add_argument('--red-band', type=int, default=1)
    ap.add_argument('--nir-band', type=int, default=2)
    ap.add_argument('--res', type=int, default=9)
    ap.add_argument('--step', type=int, default=4, help='amostra a cada N pixels (>=1)')
    ap.add_argument('--max-pixels', type=int, default=400000, help='limite aproximado de pixels processados')
    ap.add_argument('--out', dest='out_parquet', default='data/raster_h3.parquet')
    args = ap.parse_args()

    with rio.open(args.in_tif) as ds:
        red = ds.read(args.red_band).astype('float32')
        nir = ds.read(args.nir_band).astype('float32')

        
        try:
            m1 = ds.read_masks(args.red_band) > 0
            m2 = ds.read_masks(args.nir_band) > 0
            valid = m1 & m2
        except Exception:
            valid = np.isfinite(red) & np.isfinite(nir)

        ndvi = (nir - red) / (nir + red + 1e-6)
        ndvi[~valid] = np.nan

        H, W = ndvi.shape
        step = max(1, int(max(1, (H*W)//max(1,args.max_pixels)) * 0 + args.step))  
        acc = {}

        for i in range(0, H, step):
            for j in range(0, W, step):
                v = ndvi[i, j]
                if np.isnan(v): 
                    continue
                # (x=lon, y=lat)
                lon, lat = ds.xy(i, j)
                cell = to_cell(lat, lon, args.res)
                s = acc.get(cell)
                if s is None:
                    acc[cell] = [float(v), 1]
                else:
                    s[0] += float(v); s[1] += 1

    if not acc:
        raise SystemExit('Nenhuma célula acumulada (NDVI todo NaN?).')

    rows = [{'cell_h3': c, 'ndvi_mean': s[0]/s[1]} for c, s in acc.items()]
    os.makedirs(os.path.dirname(args.out_parquet) or '.', exist_ok=True)
    pd.DataFrame(rows).to_parquet(args.out_parquet, index=False)
    print(f'Gerado {args.out_parquet} com {len(rows)} linhas.')

if __name__ == '__main__':
    main()
