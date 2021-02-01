"""Geohash related utilities."""
from typing import Iterable, Tuple

import geopandas as gpd
import hvplot.pandas
import pandas as pd
from geohash import bbox
from geohash import decode as geohash_decode
from geohash import encode as geohash_encode
from geohash import expand, neighbors
from shapely.geometry import Point, Polygon, box
from spatialpandas import GeoSeries
from spatialpandas.geometry import PointArray


def geohash_encode_point(shp: Point, precision: int = 12) -> str:
    """Geohash shapely point."""
    try:
        return geohash_encode(shp.y, shp.x, precision=precision)
    except Exception:
        return None


def geohash_decode_point(geohash: str) -> Point:
    """Decode geohash to shapely point."""
    result = geohash_decode(geohash)
    return Point(result[1], result[0])


def geohash_to_box(geohash: str) -> Polygon:
    """Convert geohash to shapely box."""
    c = bbox(geohash)
    return box(c["w"], c["s"], c["e"], c["n"])


def geohash_decode_xy(geohash: str) -> Tuple[float, float]:
    """Decode geohash to `(x, y)`."""
    return geohash_decode(geohash)[::-1]


def geohashes_to_geoseries(s: pd.Series) -> GeoSeries:
    """Create spatialpandas GeoSeries from geohashes."""
    s = pd.Series(s)
    return GeoSeries(PointArray(s.apply(geohash_decode_xy)), index=s.index)


def hvplot_geohash_parents(
    geohashes: Iterable[str],
    *args,
    **kwargs,
):
    """Plot geohashes and parents."""
    l = pd.Series(geohashes).str.len().max()
    points = []
    boxes = []
    sizes = []
    alpha = []
    geohash_block = []
    for i in range(l):
        gs = set([g[:l - i] for g in geohashes if len(g) >= l - i])
        points.extend([geohash_decode_point(g) for g in gs])
        boxes.extend([geohash_to_box(g) for g in gs])
        sizes.extend([l - i] * len(gs))
        alpha.extend([(0.15 / l) * (l - i + 1)] * len(gs))
        geohash_block.extend(gs)
        if len(gs) == 1:
            break
    print(len(points), len(boxes))
    points = gpd.GeoDataFrame(
        dict(
            geometry=points,
            size=sizes,
            alpha=alpha,
            geohash=geohash_block,
        )).hvplot(
            c="size",
            cmap="viridis",
            *args,
            **kwargs,
        )
    kwargs.pop("tiles", None)
    boxes = gpd.GeoDataFrame(
        dict(
            geometry=boxes,
            size=sizes,
            alpha=alpha,
            geohash=geohash_block,
        )).hvplot(
            hover_cols=["geohash"],
            c="size",
            alpha="alpha",
            cmap="jet",
            *args,
            **kwargs,
        )
    return points * boxes


def hvplot_geohashes(
    geohashes: Iterable[str],
    *args,
    **kwargs,
):
    """Plot geohashes."""
    geohashes = pd.Series(geohashes)
    points_gdf = gpd.GeoDataFrame(
        dict(
            geometry=geohashes.apply(geohash_decode_point),
            geohash=geohashes,
        ))
    points_plot = points_gdf.hvplot(
        cmap="viridis",
        *args,
        **kwargs,
    )
    kwargs.pop("tiles", None)
    boxes_gdf = gpd.GeoDataFrame(
        dict(
            geometry=geohashes.apply(geohash_to_box),
            geohash=geohashes,
            index=geohashes.index,
        ))
    boxes_plot = boxes_gdf.hvplot(
        hover_cols=["index", "geohash"],
        alpha=0.4,
        cmap="jet",
        legend=False,
        colorbar=False,
        *args,
        **kwargs,
    )
    return points_plot * boxes_plot
