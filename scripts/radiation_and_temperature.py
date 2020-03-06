from functools import reduce
from itertools import chain
from pprint import pprint as pp
import csv
import sys

from geoalchemy2.elements import WKTElement as WKTE
from geoalchemy2.shape import to_shape
from pandas import (
    DatetimeIndex as DI,
    DataFrame as DF,
    Timedelta,
    to_datetime as tdt,
)
from shapely.geometry import Point
from sqlalchemy.orm import contains_eager as c_e
import geopandas as gpd

import feedinlib.open_FRED as open_FRED


default = open_FRED.defaultdb()
session = default["session"]
db = default["db"]


# This is just an example. Use different latitude/longitude if want data from
# a different location.
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

        - "10th of January, 09:15",
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
    limits = [tdt(time + " {}".format(year)) for year in range(2001, 2020)]
    delta = Timedelta("5d3h15m")
    limits = [(limit - delta, limit + delta) for limit in limits]
    radiation = reduce(
        lambda df1, df2: df1.append(df2),
        (
            reduce(
                lambda df1, df2: df1.join(df2, how="outer"),
                [
                    DF(
                        index=DI([stop for (_, stop, __) in tuples]),
                        data={k[1]: [value for (_, __, value) in tuples],},
                    )
                    for k in series
                    for tuples in [series[k]]
                ]
                or [DF(index=DI([]))],
            )
            for (start, stop) in limits
            for series in [
                open_FRED.Weather(
                    start=start,
                    stop=stop,
                    locations=[point],
                    variables=["ASWDIFD_S", "ASWDIR_S", "ASWDIRN_S"],
                    **default
                ).series
            ]
        ),
    )
    temperature = reduce(
        lambda d1, d2: {
            k: d1.get(k, []) + d2.get(k, []) for k in set(chain(d1, d2))
        },
        (
            open_FRED.Weather(
                start=start,
                stop=stop,
                locations=[point],
                variables=["T", "T_2M"],
                **default
            ).series
            for (start, stop) in limits
        ),
    )
    for key in temperature:
        # key[1] is the variable's name
        if key[1] == "T_2M":
            assert (key[0], "T", key[2]) not in temperature
            temperature[(key[0], "T", key[2])] = temperature[key]
            del temperature[key]
    return radiation, temperature


radiation, temperature = radiation_and_temperature("10th January 9am")

fields = ["start", "stop", "ASWDIFD_S", "ASWDIR_S", "ASWDIRN_S"]
radiation.to_csv(
    "radiation.{0[0]}-{0[1]}.csv".format(site),
    columns=fields,
    index=False,
    date_format="%Y-%m-%dT%H:%M:%S+00:00",
)

keys = sorted(set((v[0], v[1]) for v in chain(*temperature.values())))
fields = ["start", "stop"] + [
    "T: {}m".format(height) for height in sorted(k[2] for k in temperature)
]
rows = {
    (start, stop): {"start": start.isoformat(), "stop": stop.isoformat()}
    for (start, stop) in keys
}
for k in temperature:
    for start, stop, value in temperature[k]:
        rows[start, stop]["T: {}m".format(k[2])] = value
rows = [rows[k] for k in keys]

with open(
    "temperature.{0[0]}-{0[1]}.csv".format(site), "w", newline=""
) as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
