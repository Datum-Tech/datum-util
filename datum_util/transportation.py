"""Transportation analysis utilities."""

import datetime as dt
import logging
from copy import deepcopy
from datetime import timedelta
from typing import Dict

import cartopy.crs as ccrs
import colorcet as cc
import dask.dataframe as dd
import geopandas as gpd
import geoviews as gv
import holoviews as hv
import hvplot.pandas
import movingpandas as mpd
import pandas as pd
import panel as pn
import param
import spatialpandas as spd
from cartopy import crs
from geohash import bbox
from geoviews import tile_sources as gvts
from shapely.geometry import Polygon, box
from spatialpandas.io import read_parquet, read_parquet_dask, to_parquet

from .file_io import exists
from .geohash import geohash_decode_point, geohash_encode_point, geohash_to_box
from .typing import PathType

COLORMAP = deepcopy(cc.CET_L16[::-1])


def split_trajectories(
    df: pd.DataFrame,
    max_diameter: int,
    min_duration: timedelta,
    gap: timedelta,
) -> mpd.TrajectoryCollection:
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
    return all_stops


def extract_traj_info(tc: mpd.TrajectoryCollection) -> spd.GeoDataFrame:
    """Extract trajectory information from collection."""
    dfs = []
    for traj in tc.trajectories:
        traj_df = traj.df
        traj_df["traj_id"] = (
            traj.get_start_time().isoformat() + "-" + traj.df.device_id.iloc[0]
        )
        dfs.append(traj_df)
    if not dfs:
        raise ValueError(f"No trajectories found.")
    traj_dfs = pd.concat(dfs).to_crs("EPSG:2845")
    traj_collection = mpd.TrajectoryCollection(traj_dfs, "traj_id")
    data = [
        (
            traj.id,
            traj.to_linestring(),
            traj.get_direction(),
            traj.get_duration(),
            traj.get_length() / 1609.34,  # convert from m to mi
            traj.get_start_time(),
            traj.get_end_time(),
            traj.get_start_location(),
            traj.get_end_location(),
        )
        for traj in traj_collection.trajectories
    ]
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
    data["duration"] = data.duration.dt.seconds / 60
    data["start_location"] = (
        gpd.GeoDataFrame(dict(geometry=data.start_location))
        .set_crs("EPSG:2845")
        .to_crs("EPSG:4326")
    )
    data["start_geohash"] = data["start_location"].apply(geohash_encode_point)
    data["end_location"] = (
        gpd.GeoDataFrame(dict(geometry=data.end_location))
        .set_crs("EPSG:2845")
        .to_crs("EPSG:4326")
    )
    data["end_geohash"] = data["end_location"].apply(geohash_encode_point)
    data = pd.concat(
        [
            data,
            pd.DataFrame(
                data.start_location.apply(lambda p: (p.x, p.y)).to_list(),
                columns=["start_longitude", "start_latitude"],
                index=data.index,
            ),
            pd.DataFrame(
                data.end_location.apply(lambda p: (p.x, p.y)).to_list(),
                columns=["end_longitude", "end_latitude"],
                index=data.index,
            ),
        ],
        axis=1,
    )
    data = data.drop(columns=["start_location", "end_location"])
    data = data.set_crs("EPSG:2845").to_crs("EPSG:3857")
    sdf = spd.GeoDataFrame(data)
    return sdf


def append_traj_info(
    df: spd.GeoDataFrame,
    paths: Dict[str, str],
) -> spd.GeoDataFrame:
    """Append trajectory info from shape files.

    Parameters
    ----------
    df
        DataFrame containing trajectories.
    paths
        Dict containing the following paths:
            - tracts
            - tsz
            - city
            - county

    Returns
    -------
    Dataframe with appended information.

    """
    df = df.reset_index(drop=True)
    start_locations = gpd.GeoDataFrame(
        geometry=df.start_geohash.apply(geohash_decode_point),
        index=df.index,
        crs="EPSG:4326",
    )
    end_locations = gpd.GeoDataFrame(
        geometry=df.end_geohash.apply(geohash_decode_point),
        index=df.index,
        crs="EPSG:4326",
    )

    tracts = gpd.read_file(paths["tracts"]).to_crs("EPSG:4326")
    df["start_CensusBlock2019"] = (
        gpd.sjoin(
            start_locations,
            tracts[["geometry", "GEOID"]].set_index("GEOID"),
            how="left",
        )
        .rename(columns={"index_right": "start_CensusBlock2019"})
        .drop(columns=["geometry"])
        .fillna("unknown")["start_CensusBlock2019"]
        .pipe(lambda s: s.groupby(s.index).head(1))
    )
    df["end_CensusBlock2019"] = (
        gpd.sjoin(
            end_locations,
            tracts[["geometry", "GEOID"]].set_index("GEOID"),
            how="left",
        )
        .rename(columns={"index_right": "end_CensusBlock2019"})
        .drop(columns=["geometry"])
        .fillna("unknown")["end_CensusBlock2019"]
        .pipe(lambda s: s.groupby(s.index).head(1))
    )

    tsz = gpd.read_file(paths["tsz"]).to_crs("EPSG:4326")
    df["start_TSZ"] = (
        gpd.sjoin(
            start_locations,
            tsz[["geometry", "TSZ"]].set_index("TSZ"),
            how="left",
        )
        .rename(columns={"index_right": "start_TSZ"})
        .drop(columns=["geometry"])
        .fillna("Out of NCTCOG area")["start_TSZ"]
        .pipe(lambda s: s.groupby(s.index).head(1))
    )
    df["end_TSZ"] = (
        gpd.sjoin(
            end_locations,
            tsz[["geometry", "TSZ"]].set_index("TSZ"),
            how="left",
        )
        .rename(columns={"index_right": "end_TSZ"})
        .drop(columns=["geometry"])
        .fillna("Out of NCTCOG area")["end_TSZ"]
        .pipe(lambda s: s.groupby(s.index).head(1))
    )

    county = gpd.read_file(paths["county"])  # File already in EPSG:4326
    df["start_county"] = (
        gpd.sjoin(
            start_locations,
            county[["geometry", "CNTY_NM"]].set_index("CNTY_NM"),
            how="left",
        )
        .rename(columns={"index_right": "start_county"})
        .drop(columns=["geometry"])
        .fillna("unknown")["start_county"]
        .pipe(lambda s: s.groupby(s.index).head(1))
    )
    df["end_county"] = (
        gpd.sjoin(
            end_locations,
            county[["geometry", "CNTY_NM"]].set_index("CNTY_NM"),
            how="left",
        )
        .rename(columns={"index_right": "end_county"})
        .drop(columns=["geometry"])
        .fillna("unknown")["end_county"]
        .pipe(lambda s: s.groupby(s.index).head(1))
    )

    city = gpd.read_file(paths["city"])  # File already in EPSG:4326
    df["start_city"] = (
        gpd.sjoin(
            start_locations,
            city[["geometry", "CITY_NM"]].set_index("CITY_NM"),
            how="left",
        )
        .rename(columns={"index_right": "start_city"})
        .drop(columns=["geometry"])
        .fillna("unknown")["start_city"]
        .pipe(lambda s: s.groupby(s.index).head(1))
    )
    df["end_city"] = (
        gpd.sjoin(
            end_locations,
            city[["geometry", "CITY_NM"]].set_index("CITY_NM"),
            how="left",
        )
        .rename(columns={"index_right": "end_city"})
        .drop(columns=["geometry"])
        .fillna("unknown")["end_city"]
        .pipe(lambda s: s.groupby(s.index).head(1))
    )

    return df


def split_device_trajectories(
    file: PathType,
    output: PathType,
    paths: Dict[str, str],
    **kwargs,
) -> None:
    """Split device trajectories.

    Parameters
    ----------
    file: PathType
        Single parquet file containing device observations.
    output: PathType
        Base path for output.
    paths:
        Dict of paths for `append_traj_info`.

    """
    out = f"{output}/device_{file.split('.')[-2]}.parquet"
    if exists(out):
        return None
    df = pd.read_parquet(file)
    if len(df) < 2:
        return None
    tc = split_trajectories(df, **kwargs)
    sdf = extract_traj_info(tc)
    sdf = append_traj_info(sdf, paths)
    to_parquet(sdf, out)


def load_trajectory_table(root_path, year, month, espg=2845, head=None):
    """helper to load the trajectory table"""

    input_path = f"{root_path}/{year}/{month}"
    # read parquet file(s) from disk
    ddf = read_parquet_dask(input_path)
    # bring it all into memory, convert to a geopandas GeoDataFrame, and set CRS
    if head:
        gdf = ddf.head(head).to_geopandas().set_crs(f"EPSG:{espg}")
    else:
        gdf = ddf.compute().to_geopandas().set_crs(f"EPSG:{espg}")

    return gdf


def filter_by_start_time(gdf, start_date, end_date=None):
    """start_date: datetime
    TODO: this is filtering on start_date alone and ignoring end date!
    """

    # if an end date is not provided, use a single day
    if not end_date:
        end_date = start_date + dt.timedelta(days=1)

    # convert to formatted strings
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    # filter based on start_time
    filtered_gdf = gdf[gdf.start_time.between(start_str, end_str)]

    return filtered_gdf


def plot_trajectory_paths(gdf):
    """Plot the trajectory paths
    Parameters
    ----------
    gdf: geodataframe
        Reprojected trajectery gdf (in memory)
    """
    # set the crs (web mercator=3857)
    proj = ccrs.epsg(3857)
    # create plot
    plt = gdf.hvplot(
        geo=True,
        width=1000,
        height=800,
        xaxis=None,
        yaxis=None,
        cmap="fire",
        tiles="CartoLight",
        crs=proj,
    )
    return plt


def plot_geohashes(geohash_series, hash_precision=6, title=None):
    """Create a plot for a given Series of geohash strings
    (e.g. trajectory start or end geohashes).
    """
    geohash_trimmed = geohash_series.apply(lambda x: x[:hash_precision])
    geohash_counts = geohash_trimmed.value_counts()
    box_gdf = gpd.GeoDataFrame(
        [{"geometry": geohash_to_box(h), "count": c} for h, c in geohash_counts.items()]
    )

    return box_gdf.hvplot(
        geo=True,
        hover_cols=["count"],
        alpha=0.4,
        line_width=0,
        cmap=COLORMAP,
        legend=False,
        width=1000,
        height=800,
        xaxis=None,
        yaxis=None,
        tiles="CartoLight",
        title=title,
    )


def plot_trajectory_points(gdf, title=None):
    """Plot the individual points of the trajectory paths. "Spider" graph.
    Can only view one day a time before its too much.
    Parameters
    ----------
    gdf: geodataframe
        Reprojected trajectery gdf (in memory)
    """
    # set the crs (web mercator=3857)
    proj = ccrs.epsg(3857)
    # plot all of the trajectory points
    plt = gdf.hvplot.points(
        geo=True,
        datashade=True,
        legend=False,
        width=1000,
        height=800,
        xaxis=None,
        yaxis=None,
        tiles="CartoLight",
        cmap="fire",
        crs=proj,
        title=title,
        shared_axes=False,
    )
    return plt


class SingleDayView(param.Parameterized):
    """Origin/Destination heatmap viewer"""

    traj_raw = param.ClassSelector(default=gpd.GeoDataFrame(), class_=gpd.GeoDataFrame)
    traj_viewable_left = param.ClassSelector(
        default=gpd.GeoDataFrame(), class_=gpd.GeoDataFrame
    )
    traj_viewable_right = param.ClassSelector(
        default=gpd.GeoDataFrame(), class_=gpd.GeoDataFrame
    )

    start = param.Date(default=dt.datetime(2019, 1, 1))
    end = param.Date(default=dt.datetime(2019, 10, 1))

    width = param.Integer(default=800)
    height = param.Integer(default=800)

    redraw_left_trigger = param.Integer()
    redraw_right_trigger = param.Integer()

    def __init__(self, traj_raw, start, end, **params):
        self.date_slider_left = pn.widgets.DateSlider(
            name="Select Date", start=start, end=end, value=start, tooltips=False
        )
        self.date_slider_right = pn.widgets.DateSlider(
            name="Select Date", start=start, end=end, value=start, tooltips=False
        )
        super().__init__(traj_raw=traj_raw, start=start, end=end, **params)
        if start:
            self.start = start
        if end:
            self.end = end

        self.traj_viewable_left = self.apply_filter_left()
        self.traj_viewable_right = self.apply_filter_right()

    @param.depends("date_slider_left.value_throttled", watch=True)
    def apply_filter_left(self):
        """filter left df by date"""
        start_date = self.date_slider_left.value
        self.traj_viewable_left = filter_by_start_time(self.traj_raw, start_date)

        self.redraw_left_trigger += 1
        return self.traj_viewable_left

    @param.depends("date_slider_right.value_throttled", watch=True)
    def apply_filter_right(self):
        """filter right df by date"""
        start_date = self.date_slider_right.value

        self.traj_viewable_right = filter_by_start_time(self.traj_raw, start_date)

        self.redraw_right_trigger += 1
        return self.traj_viewable_right

    @param.depends("redraw_left_trigger")
    def view_left_plot(self):
        """vizualize the plot"""
        if self.traj_viewable_left.empty:
            # TODO: make this better
            plt = hv.Curve([])
        else:
            date_str = self.date_slider_left.value.strftime("%Y-%m-%d")
            plt = plot_trajectory_points(self.traj_viewable_left, title=date_str)
        return plt

    @param.depends("redraw_right_trigger")
    def view_right_plot(self):
        """vizualize the plot"""
        if self.traj_viewable_left.empty:
            # TODO: make this better
            plt = hv.Curve([])
        else:
            date_str = self.date_slider_right.value.strftime("%Y-%m-%d")
            plt = plot_trajectory_points(self.traj_viewable_right, title=date_str)
        return plt

    def panel(self):
        "view the app"

        return pn.Column(
            "### Single Day Explorer",
            pn.Row(
                pn.Spacer(width=30),
                pn.Column(
                    self.view_left_plot,
                    self.date_slider_left,
                ),
                pn.Column(
                    self.view_right_plot,
                    self.date_slider_right,
                ),
            ),
        )


class ODViewer(param.Parameterized):
    """Origin/Destination heatmap viewer
    TODO: add geoehash as input
          xlim/ylim
    """

    traj_raw = param.ClassSelector(default=gpd.GeoDataFrame(), class_=gpd.GeoDataFrame)
    traj_viewable_left = param.ClassSelector(
        default=gpd.GeoDataFrame(), class_=gpd.GeoDataFrame
    )
    traj_viewable_right = param.ClassSelector(
        default=gpd.GeoDataFrame(), class_=gpd.GeoDataFrame
    )

    start = param.Date(default=dt.datetime(2019, 1, 1))
    end = param.Date(default=dt.datetime(2019, 10, 1))

    width = param.Integer(default=800)
    height = param.Integer(default=800)

    redraw_left_trigger = param.Integer()
    redraw_right_trigger = param.Integer()

    def __init__(self, traj_raw, start, end, **params):
        self.date_slider = pn.widgets.DateRangeSlider(
            name="Select Date",
            start=start,
            end=end,
            value=(start, start + dt.timedelta(days=1)),
            tooltips=False,
        )
        super().__init__(traj_raw=traj_raw, start=start, end=end, **params)
        if start:
            self.start = start
        if end:
            self.end = end
        self.traj_viewable_left = self.apply_filter_left()
        self.traj_viewable_right = self.apply_filter_right()

    @param.depends("date_slider.value_throttled", watch=True)
    def apply_filter_left(self):
        """filter left df by date"""
        start_date, end_date = self.date_slider.value
        if start_date == end_date:
            end_date = start_date + dt.timedelta(days=1)
        self.traj_viewable_left = filter_by_start_time(
            self.traj_raw, start_date, end_date=end_date
        )

        self.redraw_left_trigger += 1
        return self.traj_viewable_left

    @param.depends("date_slider.value_throttled", watch=True)
    def apply_filter_right(self):
        """filter right df by date"""
        start_date, end_date = self.date_slider.value
        if start_date == end_date:
            end_date = start_date + dt.timedelta(days=1)
        self.traj_viewable_right = filter_by_start_time(
            self.traj_raw, start_date, end_date=end_date
        )

        self.redraw_right_trigger += 1
        return self.traj_viewable_right

    @param.depends("redraw_left_trigger")
    def view_left_plot(self):
        """vizualize the plot"""
        if self.traj_viewable_left.empty:
            # TODO: make this better
            plt = hv.Curve([])
        else:
            plt = plot_geohashes(
                self.traj_viewable_left["start_geohash"], 6, title="Trip Origin"
            )
        return plt

    @param.depends("redraw_right_trigger")
    def view_right_plot(self):
        """vizualize the plot"""
        if self.traj_viewable_right.empty:
            # TODO: make this better
            plt = hv.Curve([])
        else:
            plt = plot_geohashes(
                self.traj_viewable_right["end_geohash"], 6, title="Trip Destination"
            )
        return plt

    def panel(self):
        "view the app"

        return pn.Column(
            pn.Row(
                pn.Spacer(width=30),
                pn.Column(
                    self.view_left_plot,
                ),
                pn.Column(
                    self.view_right_plot,
                ),
            ),
            self.date_slider,
        )


class TripSegmentation(param.Parameterized):

    device_selector = param.ObjectSelector(label='Select a Device')
    trip_table = param.DataFrame()
    source_data = param.DataFrame()
    single_device_trips = param.DataFrame()

    plot_height = 800
    plot_width =1000

    max_diameter = param.Integer(default=30, label='Stop diameter (meters)')
    min_duration = param.Integer(default=5, label='Stop time (min)')
    gap = param.Integer(default=60, label='Trip gap time (min)')

    action = param.Action(lambda x: x.param.trigger('action'), label='Recompute Trips')

    def __init__(self, source_data, **params):
        super().__init__(source_data=source_data, **params)

        self.source_data = source_data

        # process the source files to get the trip table
        self.process_trajectories()

    def reset_device_dropdown(self):
        # get the list of unique devices
        device_list = self.trip_table["device_id"].unique()
        self.param.device_selector.objects = device_list
        self.device_selector = device_list[0]

    @param.depends('action', watch=True)
    def process_trajectories(self):
        tc = split_trajectories(self.source_data, max_diameter=self.max_diameter,  min_duration=timedelta(minutes=self.min_duration), gap=timedelta(minutes=self.gap))
        self.trip_table = extract_traj_info(tc)
        self.reset_device_dropdown()

    @param.depends('device_selector')
    def view_device_trips(self):
        """plot the trips for the selected device, each in its own color"""
        # get the rows for the selected device only
        single = self.trip_table[self.trip_table["device_id"] == self.device_selector]
        # create a colormap for each trajectory
        explicit_map = {}
        for idx, traj in enumerate(single['traj_id']):
            explicit_map[traj] = cc.palette.glasbey_bw[idx]

        # construct plots for each trip
        path_plts = []
        for traj in single['traj_id']:
            geom = single[single['traj_id'] == traj]['geometry']
            path_plts.append(gv.Path(geom, crs=ccrs.GOOGLE_MERCATOR).opts(color=explicit_map[traj]))
        # overlay the plots on tiles
        plt = gvts.CartoLight() * hv.Overlay(path_plts)

        return plt.opts(height=self.plot_height, width=self.plot_width)

    def panel(self):
        return pn.Column(
            pn.Accordion(
                (
                    'Select Trip Segmentation Options',
                    pn.Column(
                        self.param.max_diameter,
                        self.param.min_duration,
                        self.param.gap,
                        self.param.action,
                        width=1100,
                    )
                ),
                (
                    'View Results',
                    pn.Column(
                        self.param.device_selector,
                        self.view_device_trips,
                        width=1100,
                    ),
                ),
            ),

        )
