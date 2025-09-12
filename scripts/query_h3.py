#!/usr/bin/env python
# Consulta H3: join de vetor + raster por celula, filtros básicos e export

import argparse, os, sys, json
import pandas as pd

def load_df(path, required_cols):
    if not os.path.exists(path):
        raise SystemExit(f"Arquivo não encontrado: {path}")
    df = pd.read_parquet(path)
    miss = [c for c in required_cols if c not in df.columns]
    if miss:
        raise SystemExit(f"Colunas ausentes em {path}: {miss}")
    return df

def main():
    ap = argparse.ArgumentParser(description="Query H3 (vector + raster)")
    ap.add_argument("--vector", default="data/vector_h3.parquet")
    ap.add_argument("--raster", default="data/raster_h3.parquet")
    ap.add_argument("--out", default="reports/joined_h3.parquet")
    ap.add_argument("--out-csv", default=None)
    ap.add_argument("--filter-cells", default=None, help="Caminho p/ JSON com lista de cells H3")
    ap.add_argument("--agg-by", default=None, help="Coluna do vetor p/ agregar (ex.: tipo)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    dv = load_df(args.vector, ["cell_h3"])
    dr = load_df(args.raster, ["cell_h3", "ndvi_mean"])

    if args.filter_cells:
        cells = json.load(open(args.filter_cells, "r", encoding="utf-8"))
        if not isinstance(cells, (list, tuple)) or not cells:
            raise SystemExit("filter-cells deve ser JSON com lista de H3 cells.")
        dv = dv[dv["cell_h3"].isin(cells)]
        dr = dr[dr["cell_h3"].isin(cells)]

    df = pd.merge(dv, dr, on="cell_h3", how="inner")

    if df.empty:
        print("Sem resultados após o join.")
        sys.exit(0)

    # Resumos simples
    print(f"Linhas no join: {len(df)}")
    print(f"NDVI média: {df['ndvi_mean'].mean():.4f}  |  min: {df['ndvi_mean'].min():.4f}  |  max: {df['ndvi_mean'].max():.4f}")

    # Agregação opcional por uma coluna do vetor
    if args.agg_by and args.agg_by in df.columns:
        grp = df.groupby(args.agg_by)["ndvi_mean"].agg(["count", "mean", "min", "max"]).reset_index()
        print("\nAgregado por", args.agg_by)
        print(grp.head(20).to_string(index=False))
        # salva também
        base, ext = os.path.splitext(args.out)
        agg_out = base + f".agg_by_{args.agg_by}" + ext
        grp.to_parquet(agg_out, index=False)
        if args.out_csv:
            grp.to_csv(os.path.splitext(args.out_csv)[0] + f".agg_by_{args.agg_by}.csv", index=False)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    df.to_parquet(args.out, index=False)
    if args.out_csv:
        df.to_csv(args.out_csv, index=False)
    if args.verbose:
        print(f"Salvo: {args.out}  |  {args.out_csv or '(sem CSV)'}")

if __name__ == "__main__":
    main()
