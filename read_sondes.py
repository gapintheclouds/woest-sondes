# Adapted from https://github.com/joshua-hampton/woest-plotting/blob/main/radiosonde_plots/do_radiosonde.py

import polars as pl
import datetime as dt
import os


def do_radiosondes(file_name, outdir):
    radiosonde_metadata = {}
    radiosonde_metadata["date"] = file_name.split("/")[-1].split("_")[1]
    radiosonde_metadata["time"] = file_name.split("/")[-1].split("_")[2].split(".")[0]

    with open(file_name, encoding="charmap") as f:
        line_skip = 0
        units_line_next = False
        while True:
            data = f.readline()
            if units_line_next:
                data_units = [i.strip() for i in data.split("\t")]
                break
            elif data.startswith("Elapsed time") or data.startswith(" TimeUTC"):
                units_line_next = True
            elif "\t" in data and not units_line_next:
                data_key = data.split("\t")[0].strip()
                data_value = data.split("\t")[1].strip()
                radiosonde_metadata[data_key] = data_value
                line_skip += 1
            else:
                line_skip += 1

    df = pl.read_csv(
        file_name,
        encoding="charmap",
        skip_rows=line_skip,
        separator="\t",
        skip_rows_after_header=1,
        ignore_errors=True,
    ).fill_null(float("nan"))

    for column_name in df.columns:
        df = df.rename({column_name: column_name.strip()})

    # Remove empty lines in csv file
    df = df.filter(pl.col("TimeUTC") != '')


    df_small = df.select(
        [
            # "Elapsed time",
            "HeightMSL",
            "RH",
            "Lat",
            "Lon",
            "P",
            "Temp",
            "Dir",
            "Speed",
        ]
    )

    df_small_renamed = df_small.rename(
        {
            # "Elapsed time": "DataSrvTime",
            "HeightMSL": "Height",
            "RH": "Humidity",
            "Lat": "Latitude",
            "Lon": "Longitude",
            "P": "Pressure",
            "Temp": "Temperature",
            "Dir": "WindDir",
            "Speed": "WindSpeed",
        }
    )

    csv_filename = (
        f"{radiosonde_metadata['System trademark and model']}-{radiosonde_metadata['Station name']}-"
        f"{radiosonde_metadata['date']}-"
        f"{radiosonde_metadata['time']}-radiosonde.csv"
    )
    # df_small_renamed.write_csv(file=f"{os.path.split(file_name)[0]}/{csv_filename}")

    radiosonde_metadata["start_time_dt"] = dt.datetime.strptime(
        radiosonde_metadata["Balloon release date and time"], "%Y-%m-%dT%H:%M:%S"
    )

    radiosonde_metadata["sonde_system_owner"] = 'Test'
    radiosonde_metadata["sonde_system_operator"] = 'Test'

    # Might need pl.fill_nan here eg: https://docs.pola.rs/user-guide/expressions/null/#notanumber-or-nan-values
    # Depends on what happens when saving as NetCDF
    # Missing data eg. '//////' is currently replaced with 'null'
    # remove ignore_errors in the read_csv line to see it crash over missing data

    # print(radiosonde_metadata)
    # print(data_units)

    return df, radiosonde_metadata, data_units


if __name__ == "__main__":
    import sys

    file_name = sys.argv[1]
    outdir = sys.argv[2]
    df, radiosonde_metadata, data_units = do_radiosondes(file_name, outdir)
