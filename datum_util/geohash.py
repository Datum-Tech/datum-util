"""Geohash related utilities."""
from geohash import bbox
from geohash import decode as geohash_decode
from geohash import encode as geohash_encode
from shapely.geometry import box, Point, Polygon


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
