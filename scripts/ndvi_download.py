#!/usr/bin/env python
import os, sys, time, json, warnings
import numpy as np
import rasterio
from rasterio.windows import from_bounds
from rasterio.warp import transform_bounds
from pystac_client import Client
import planetary_computer

from shapely.geometry import box, shape
from shapely.ops import unary_union

# ---- Config por ENV (com defaults seguros) ----
MG_BBOX = tuple(map(float, os.getenv("MG_BBOX", "-51 -23.5 -39.5 -14").split()))
DATE    = os.getenv("NDVI_DATE", "2023-07-01/2023-08-31")
CLOUD   = float(os.getenv("NDVI_MAX_CLOUD", "20"))

OUT_REDNIR = "data/mg_s2_rednir.tif"
OUT_NDVI   = "data/mg_s2_ndvi.tif"

def log(msg): print(f"[downloader] {msg}", flush=True)

def main():
    t0 = time.perf_counter()
    os.makedirs("data", exist_ok=True)

    log(f"bbox(WGS84)={MG_BBOX} date={DATE} cloud<{CLOUD}%")
    cat = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

    # busca inicial (relaxa depois se precisar)
    limits = [(CLOUD, DATE),
              (30, DATE),
              (50, DATE),
              (50, "2023-01-01/2023-12-31"),
              (70, "2022-01-01/2024-12-31")]
    mg_poly = box(*MG_BBOX)

    chosen = None
    chosen_intersection = None

    for cloud_max, date_range in limits:
        log(f"search: cloud<{cloud_max}% date={date_range}")
        search = cat.search(
            collections=["sentinel-2-l2a"],
            bbox=MG_BBOX,
            datetime=date_range,
            query={"eo:cloud_cover": {"lt": cloud_max}},
            limit=50,
        )
        items = list(search.items())
        log(f"  found={len(items)}")
        if not items:
            continue

        # ordenar por nuvem crescente
        items.sort(key=lambda it: it.properties.get("eo:cloud_cover", 100))

        # pegar o primeiro cujo footprint intersete MG
        for it in items:
            try:
                geom = it.geometry or {"type": "Polygon", "coordinates": [ [[it.bbox[0],it.bbox[1]],[it.bbox[2],it.bbox[1]],[it.bbox[2],it.bbox[3]],[it.bbox[0],it.bbox[3]],[it.bbox[0],it.bbox[1]]] ]}
                it_poly = shape(geom)
            except Exception:
                # fallback para bbox do item
                it_poly = box(*it.bbox)

            inter = it_poly.intersection(mg_poly)
            if not inter.is_empty and inter.area > 0:
                chosen = it
                chosen_intersection = inter.bounds  # still in WGS84
                log(f"  chosen id={it.id} cloud={it.properties.get('eo:cloud_cover')} inter={chosen_intersection}")
                break

        if chosen:
            break

    if not chosen:
        print("Nenhuma cena útil (sem interseção com MG). Ajuste a janela ou filtros.", file=sys.stderr)
        sys.exit(2)

    # assinar e pegar hrefs
    it = planetary_computer.sign(chosen)
    red_href = it.assets["B04"].href  # Red 10m
    nir_href = it.assets["B08"].href  # NIR 10m

    log("reading COGs via HTTP…")
    with rasterio.Env(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
        GDAL_HTTP_TIMEOUT="45",
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif,.TIF",
    ):
        # abrimos apenas para pegar CRS/transform e calcular janela reprojetada
        with rasterio.open(red_href) as red_ds:
            # reprojetar o bbox de interseção MG∩item para o CRS do dataset
            inter_wgs84 = chosen_intersection  # (minx,miny,maxx,maxy) em EPSG:4326
            inter_in_ds = transform_bounds("EPSG:4326", red_ds.crs, *inter_wgs84, densify_pts=21)

            # janela no CRS do dataset
            win = from_bounds(*inter_in_ds, transform=red_ds.transform)
            if win.width <= 0 or win.height <= 0:
                log(f"  window is empty for this item, trying next…")
                print("Janela 0x0 — tente outra data ou bbox.", file=sys.stderr)
                sys.exit(3)

            # ler a janela
            t1 = time.perf_counter()
            red = red_ds.read(1, window=win).astype("float32")
            transform = red_ds.window_transform(win)
            crs = red_ds.crs

        with rasterio.open(nir_href) as nir_ds:
            nir = nir_ds.read(1, window=win).astype("float32")

        log(f"read window done in {time.perf_counter()-t1:.1f}s  shape={red.shape}")

    # salvar 2-bandas
    t2 = time.perf_counter()
    with rasterio.open(
        OUT_REDNIR, "w", driver="GTiff",
        height=red.shape[0], width=red.shape[1], count=2,
        dtype="float32", crs=crs, transform=transform,
        TILED=True, COMPRESS="LZW", BIGTIFF="IF_SAFER"
    ) as dst:
        dst.write(red, 1); dst.write(nir, 2)
    log(f"saved {OUT_REDNIR} in {time.perf_counter()-t2:.1f}s")

    # opcional: NDVI
    np.seterr(divide='ignore', invalid='ignore')
    ndvi = (nir - red) / (nir + red)
    ndvi = np.clip(ndvi, -1.0, 1.0).astype("float32")

    t3 = time.perf_counter()
    with rasterio.open(
        OUT_NDVI, "w", driver="GTiff",
        height=ndvi.shape[0], width=ndvi.shape[1], count=1,
        dtype="float32", crs=crs, transform=transform,
        TILED=True, COMPRESS="LZW", BIGTIFF="IF_SAFER"
    ) as dst:
        dst.write(ndvi, 1)
    log(f"saved {OUT_NDVI} in {time.perf_counter()-t3:.1f}s")

    print(json.dumps({
        "bbox_WGS84": MG_BBOX,
        "date": DATE, "cloud_lt": CLOUD,
        "chosen_item": chosen.id,
        "outputs": {"rednir": OUT_REDNIR, "ndvi": OUT_NDVI},
        "seconds": round(time.perf_counter() - t0, 1)
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=FutureWarning)
        main()

