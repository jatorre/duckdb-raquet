#!/usr/bin/env python3
"""Regenerate test/data/test_palette.tif used by the metadata enrichment tests.

Produces a 16x16 single-band uint8 GeoTIFF with a 4-entry color table and a
PaletteIndex color interpretation. The fixture is small (~2 KB) and stable
across runs (numpy seed=42), which keeps the SQL goldens in
test/sql/read_raster_metadata.test deterministic.
"""

import os

from osgeo import gdal, osr
import numpy as np


def main() -> None:
    out_path = os.path.join(
        os.path.dirname(__file__), "..", "test", "data", "test_palette.tif"
    )
    out_path = os.path.normpath(out_path)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(out_path, 16, 16, 1, gdal.GDT_Byte,
                       options=["COMPRESS=DEFLATE"])

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    ds.SetProjection(srs.ExportToWkt())
    ds.SetGeoTransform([13.0, 0.001, 0.0, 52.5, 0.0, -0.001])

    band = ds.GetRasterBand(1)
    ct = gdal.ColorTable()
    ct.SetColorEntry(0, (0, 0, 0, 0))
    ct.SetColorEntry(1, (12, 68, 28, 255))
    ct.SetColorEntry(2, (200, 170, 90, 255))
    ct.SetColorEntry(3, (60, 120, 200, 255))
    band.SetColorTable(ct)
    band.SetColorInterpretation(gdal.GCI_PaletteIndex)
    band.SetNoDataValue(0)

    rng = np.random.default_rng(42)
    band.WriteArray(rng.integers(0, 4, size=(16, 16), dtype=np.uint8))
    ds.FlushCache()
    ds = None

    print(f"wrote {out_path} ({os.path.getsize(out_path)} bytes)")


if __name__ == "__main__":
    main()
