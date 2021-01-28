"""Transportation analysis utilities."""
from datetime import timedelta

import geopandas as gpd
import movingpandas as mpd
import pandas as pd
import spatialpandas as spd

from .geohash import geohash_encode_point


def split_trajectories(
    df: pd.DataFrame,
    max_diameter: int,
    min_duration: timedelta,
    gap: timedelta,
) -> spd.GeoDataFrame:
    """Split trajectories."""
    gdf = gpd.GeoDataFrame(
        df.reset_index().set_index("timestamp"),
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs="epsg:4326",
    )
    traj_collection = mpd.TrajectoryCollection(gdf, "device_id")
    stops = mpd.StopSplitter(traj_collection).split(
        max_diameter=max_diameter,
        min_duration=min_duration,
    )
    all_stops = mpd.ObservationGapSplitter(stops).split(gap=gap)
    dfs = []
    for traj in all_stops.trajectories:
        traj_df = traj.df
        traj_df["traj_id"] = (traj.get_start_time().isoformat() + "-" +
                              traj.df.device_id.iloc[0])
        dfs.append(traj_df)
    if not dfs:
        raise ValueError(f"No trajectories found, number points: {len(df)}")
    traj_dfs = pd.concat(dfs).to_crs("EPSG:2845")
    traj_collection = mpd.TrajectoryCollection(traj_dfs, "traj_id")
    data = [(
        traj.id,
        traj.to_linestring(),
        traj.get_direction(),
        traj.get_duration(),
        traj.get_length(),
        traj.get_start_time(),
        traj.get_end_time(),
        traj.get_start_location(),
        traj.get_end_location(),
    ) for traj in traj_collection.trajectories]
    data = gpd.GeoDataFrame(
        data,
        columns=[
            "traj_id",
            "geometry",
            "direction",
            "duration",
            "length",
            "start_time",
            "end_time",
            "start_location",
            "end_location",
        ],
    )
    data["device_id"] = data.traj_id.str.split("-").str[-1]
    data["duration"] = data.duration.dt.seconds
    data["start_geohash"] = (gpd.GeoDataFrame(
        dict(geometry=data.start_location)).set_crs("EPSG:2845").to_crs(
            "EPSG:4326").geometry.apply(geohash_encode_point))
    data["end_geohash"] = (gpd.GeoDataFrame(
        dict(geometry=data.end_location)).set_crs("EPSG:2845").to_crs(
            "EPSG:4326").geometry.apply(geohash_encode_point))
    data = data.drop(columns=["start_location", "end_location"])
    data = data.set_crs("EPSG:2845").to_crs("EPSG:3857")
    sdf = spd.GeoDataFrame(data)
    return sdf
