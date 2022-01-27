import arrow
from influxdb import InfluxDBClient
import pandas as pd
import numpy as np

def influxdb_get_data(self, entity_id, unit, start_time="", end_time="", value="value"):
    if start_time != "":
        start_time = " AND time > " + start_time
    if end_time != "":
        end_time = " AND time < " + end_time
    result = self.query(f'SELECT { value }  FROM "{ self._database }"."autogen"."{ unit }" WHERE "entity_id"=\'' +
                 f"{ entity_id }'{start_time + end_time}")
    data = [p[value] for p in result.get_points()]
    index = pd.DatetimeIndex([p['time'] for p in result.get_points()])
    return pd.Series(data, index, name=entity_id, dtype=np.float64)

def influxdb_write_data(self, series, unit, entity_id=None):
    data = []
    if entity_id is None:
        entity_id = series.name
    for ts, row in series.dropna().iteritems():
        data.append({
            "measurement": unit,
            "tags": {
                "entity_id": entity_id,
                "domain": "sensor",
            },
            "fields": {
                "value": row,
            },
            "time": ts.isoformat().split("+")[0] + "Z"
        })
    self.write_points(data)

InfluxDBClient.get_data = influxdb_get_data
InfluxDBClient.write_data = influxdb_write_data

def get_gridwatch_data():
    df = pd.read_csv("https://raw.githubusercontent.com/ryanfobel/gridwatch-history/main/data/summary.csv", index_col=0)
    units = []
    mapper = {}
    for col in df.columns:
        if "%" in df[col].iloc[0]:
            units = "%"
            df[col] = [float(x[:-1].replace(",", "")) for x in df[col]]
        else:
            units = df[col].iloc[0].split(" ")[-1]
            df[col] = [float(x.split(" ")[0].replace(",", "")) for x in df[col]]
        mapper[col] = col + f" ({units})"
    df.rename(columns=mapper, inplace=True)

    def convert_timestamp(t, year):
        month, day, hour, suffix = t.split(" ")[1:5]
        hour = int(hour) + [12,0][suffix == "AM"]
        return arrow.get("%s %s, %d %02d:00:00" % (month, day[:-1], year, int(hour)), "MMM D, YYYY HH:mm:ss").datetime.replace(tzinfo=None)

    df["Year"] = 2021
    prev_month = df.index[0].split(" ")[1]
    for iloc, (loc, row) in enumerate(df[1:].iterrows()):
        month = loc.split(" ")[1]
        prev_year = df.iloc[iloc-1]["Year"]
        if month == "Jan" and prev_month != "Jan":
            df.loc[loc:, "Year"] = prev_year + 1
        prev_month = month

    df["Datetime"] = None
    for iloc, (loc, row) in enumerate(df.iterrows()):
        month, day, hour, suffix = loc.split(" ")[1:5]
        hour = int(hour) + [12,0][suffix == "AM"]
        df.loc[loc, "Datetime"] = convert_timestamp(loc, row["Year"])

    df.drop(columns="Year", inplace=True)
    df.set_index("Datetime", inplace=True)
    df.sort_index(inplace=True)
    return df
