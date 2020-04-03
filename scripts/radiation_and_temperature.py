from functools import reduce
from itertools import chain
from pprint import pprint as pp
import sys

from geoalchemy2.elements import WKTElement as WKTE
from geoalchemy2.shape import to_shape
from pandas import (
    DatetimeIndex as DI,
    DataFrame as DF,
    Interval,
    IntervalIndex as II,
    Timedelta,
    date_range,
    to_datetime as tdt,
)
from shapely.geometry import Point
from sqlalchemy.orm import contains_eager as c_e
import geopandas as gpd

import feedinlib.open_FRED as open_FRED


default = open_FRED.defaultdb()
session = default["session"]
db = default["db"]


# This is just an example. Use a different latitude/longitude if you want data
# from a different location.
point = Point(9.7311, 53.3899)

# The chosen point is probably not among the give measurement sites, so we need
# to figure out wich measurement site is closest to the given point.
# The corresponding code is copied from [`feedinlib.open_FRED.location`][0].
#
# [0]: https://github.com/oemof/feedinlib/blob/a0094c7d277b289cb6f5222d184d081b0f7eea5f/feedinlib/open_FRED.py#L301
site = (lambda p: (p.x, p.y))(
    to_shape(
        session.query(db["Location"])
        .order_by(
            db["Location"].point.distance_centroid(
                WKTE(point.to_wkt(), srid=4326)
            )
        )
        .first()
        .point
    )
)


def radiation_and_temperature(time):
    """ Gets all radiation and temperature data available for the `time`.

    Just supply a time, like e.g.

        - "10th of January, 09:00",
        - "January 10th, 9am" or even
        - "01/10 9am"

    and the function will return matching time series data in the range
        ```
        time - (5 days and 3 hours)
        ```
    for each available year.
    """
    # This is the minimum start and the maximum stop one could query:
    #   2001-12-31T23:00:00
    #   2019-01-01T00:00:00
    timepoints = [tdt(time + " {}".format(year)) for year in range(2001, 2020)]
    delta = Timedelta("5d2h15m")
    df = reduce(
        lambda df1, df2: df1.append(df2),
        (
            reduce(
                lambda df1, df2: df1.join(df2, how="outer"),
                [
                    DF(
                        index=DI([stop for (_, stop, __) in tuples]),
                        data={
                            (
                                k[1]
                                if not k[2]
                                else "{}: {}m".format(k[1], k[2])
                            ): [value for (_, __, value) in tuples],
                        },
                    )
                    for k in series
                    for tuples in [series[k]]
                ]
                or [DF(index=DI([]))],
            )
            .resample("30min")
            .apply(lambda xs: xs.sum() / len(xs))
            for tp in timepoints
            for (start, stop) in [(tp - delta, tp + delta)]
            for series in [
                open_FRED.Weather(
                    start=start,
                    stop=stop,
                    locations=[point],
                    heights=[10],
                    variables=[
                        "ASWDIFD_S",
                        "ASWDIR_S",
                        "ASWDIRN_S",
                        "T",
                        # "T_2M",
                    ],
                    **default
                ).series
            ]
        ),
    )
    ydelta, daydelta = Timedelta("5d"), Timedelta("2h15m")
    days = chain(
        *(date_range(tp - ydelta, tp + ydelta, freq="1d") for tp in timepoints)
    )
    ii = II(data=[Interval(day - daydelta, day + daydelta) for day in days])
    df = df[[ii.contains(ix) for ix in df.index]]
    return df


df = radiation_and_temperature("10th January 9am")

fields = [
    "ASWDIFD_S",
    "ASWDIR_S",
    "ASWDIRN_S",
    "T: 10.0m",
]
df.to_csv(
    "rat.{0[0]}-{0[1]}.csv".format(site),
    columns=fields,
    date_format="%Y-%m-%dT%H:%M:%S+00:00",
)
