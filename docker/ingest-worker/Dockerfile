
FROM ghcr.io/osgeo/gdal:ubuntu-small-latest

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-venv && \
    rm -rf /var/lib/apt/lists/*

# venv para não esbarrar no PEP 668
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

# libs Python (inclui drivers de I/O: fiona e pyogrio)
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir \
      numpy pandas pyarrow \
      shapely pyproj geopandas rasterio \
      fiona pyogrio \
      fastapi h3 pydeck

RUN useradd -m ingestuser
USER ingestuser
WORKDIR /app

ENTRYPOINT ["python"]

