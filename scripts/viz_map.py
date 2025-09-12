#!/usr/bin/env python
# Mapa NDVI por H3 (pydeck) 

import argparse, os
import pandas as pd
import numpy as np
import pydeck as pdk
import h3

def cell_center(cell):
    if hasattr(h3, "cell_to_latlng"):
        lat, lng = h3.cell_to_latlng(cell)
    else:
        lat, lng = h3.h3_to_geo(cell)
    return float(lat), float(lng)

def make_color_scale(vals):
    v = np.asarray(vals, dtype="float32")
    v = v[np.isfinite(v)]
    if v.size == 0:
        vmin, vmax = 0.0, 1.0
    else:
        vmin, vmax = np.quantile(v, 0.05), np.quantile(v, 0.95)
        if vmin == vmax: vmin, vmax = float(v.min()), float(v.max())
    t = (np.asarray(vals) - vmin) / (vmax - vmin + 1e-12)
    t = np.clip(t, 0, 1)
    colors = []
    for w in t:
        if w < 0.5:
            w2 = w/0.5; r=165; g=int(0+(191-0)*w2); b=int(38+(0-38)*w2)
        else:
            w2 = (w-0.5)/0.5; r=int(165+(0-165)*w2); g=int(191+(104-191)*w2); b=int(0+(55-0)*w2)
        colors.append([r,g,b,190])
    return colors, (float(vmin), float(vmax))

def compute_view(cells):
    latlng = np.array([cell_center(c) for c in cells])
    lat_min, lng_min = latlng.min(axis=0); lat_max, lng_max = latlng.max(axis=0)
    lat0 = (lat_min + lat_max)/2; lng0 = (lng_min + lng_max)/2
    span = max(lat_max-lat_min, lng_max-lng_min)
    zoom = 14 if span < 0.01 else 12 if span < 0.03 else 10 if span < 0.1 else 8
    return pdk.ViewState(latitude=lat0, longitude=lng0, zoom=zoom)

def main():
    ap = argparse.ArgumentParser(description="Mapa NDVI por H3 (pydeck)")
    ap.add_argument("--in", dest="in_parquet", default="reports/joined_h3.parquet")
    ap.add_argument("--out", dest="out_html", default="reports/map_ndvi.html")
    ap.add_argument("--opacity", type=float, default=0.85)
    ap.add_argument("--elevation", type=float, default=0.0)
    ap.add_argument("--height", type=int, default=720)
    ap.add_argument("--mapbox-key", default=None, help="Token Mapbox (opcional; senão usa env MAPBOX_API_KEY)")
    args = ap.parse_args()

    df = pd.read_parquet(args.in_parquet)
    if "cell_h3" not in df.columns or "ndvi_mean" not in df.columns:
        raise SystemExit("Parquet precisa ter cell_h3 e ndvi_mean")
    df = df.groupby("cell_h3", as_index=False).agg({"ndvi_mean":"mean"})

    colors, (vmin, vmax) = make_color_scale(df["ndvi_mean"].to_numpy())
    df["_color"] = colors; df["_line_color"] = [[40,40,40,120]]*len(df)

    view_state = compute_view(df["cell_h3"])

    # Mapbox key (CLI tem prioridade; senão ENV; senão None)
    key = args.mapbox_key or os.environ.get("MAPBOX_API_KEY")
    if key:
        # garantir que pydeck saiba do token
        pdk.settings.map_provider = "mapbox"
        pdk.settings.mapbox_api_key = key
        map_style = "mapbox://styles/mapbox/light-v10"
    else:
        map_style = None

    hex_layer = pdk.Layer(
        "H3HexagonLayer",
        df,
        pickable=True, auto_highlight=True,
        get_hexagon="cell_h3",
        get_fill_color="_color",
        get_line_color="_line_color",
        stroked=True, line_width_min_pixels=1,
        extruded=bool(args.elevation > 0),
        get_elevation=f"ndvi_mean * {args.elevation}" if args.elevation > 0 else 0,
        opacity=args.opacity,
    )

    tooltip = {"html":"<b>H3:</b> {cell_h3}<br/><b>NDVI:</b> {ndvi_mean}",
               "style":{"backgroundColor":"rgba(30,30,30,0.9)","color":"white"}}

    deck = pdk.Deck(layers=[hex_layer], initial_view_state=view_state,
                    map_style=map_style, tooltip=tooltip, height=args.height)

    deck.description = f"NDVI ~ quantis 5–95% | min={vmin:.3f} | max={vmax:.3f}"
    os.makedirs(os.path.dirname(args.out_html) or ".", exist_ok=True)
    deck.to_html(args.out_html, notebook_display=False)
    print(f"Mapa salvo em {args.out_html} ({len(df)} células). Mapbox={'ON' if key else 'OFF'}.")

if __name__ == "__main__":
    main()

