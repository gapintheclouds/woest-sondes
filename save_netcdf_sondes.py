from read_sondes import do_radiosondes
import glob
import netCDF4 as nc
import datetime as dt
import numpy as np
import polars as pl


# Class with all the sonde information in.

class SondeInfo:

    def __init__(self, site_name='default'):

        self.name = site_name

        if self.name == 'AshFarm':
            self.system_owner = 'Atmospheric Measurement and Observation Facility'
            self.instrument_name = 'ncas-radiosonde-1'
            self.system_operator = 'University of Leeds'

        if self.name == 'Chilbolton':
            self.system_owner = 'Met Office'
            self.instrument_name = 'ukmo-radiosonde'
            self.system_operator = 'Met Office'

        if self.name == 'LAR_A':
            self.system_owner = 'Met Office'
            self.instrument_name = 'ukmo-radiosonde'
            self.system_operator = 'Met Office'

        if self.name == 'Larkhill_B':
            self.system_owner = 'Met Office'
            self.instrument_name = 'ukmo-radiosonde'
            self.system_operator = 'Met Office'

        if self.name == 'Reading':
            self.system_owner = 'University of Reading'
            self.instrument_name = 'uor-radiosonde'
            self.system_operator = 'University of Reading'

        if self.name == 'SpireView':
            self.system_owner = 'Met Office'
            self.instrument_name = 'ukmo-radiosonde'
            self.system_operator = 'Met Office'


def save_netcdf_file(df, radiosonde_metadata, netcdf_dir):
    # Replace nulls with NaNs
    this_fill_value = -1.00e+20

    # Convert time string to datetime
    start_date = f"{radiosonde_metadata['start_time_dt']:%Y-%m-%d}"
    sonde_time_dt = []

    for this_time in df["TimeUTC"]:
        sonde_time_dt.append(dt.datetime.strptime(f"{start_date}T{this_time}", "%Y-%m-%dT%H:%M:%S"))

    # Load sonde system metadata
    sonde_system_info = SondeInfo(radiosonde_metadata['Station name'])
    # Set up file name
    # use format: radiosonde_woest_ashfarm_20231010_112200_v1
    date_string = radiosonde_metadata['start_time_dt'].strftime("%Y%m%d-%H%M%S")
    product_version_number = 'v0.1'
    software_version_number = 'v0.1'
    nc_filename = (f"{sonde_system_info.instrument_name}_{radiosonde_metadata['Station name'].lower()}_{date_string}_"
                   f"sonde_woest_{product_version_number}.nc")
    current_time = dt.datetime.now(dt.timezone.utc)
    current_time_string = current_time.strftime('%Y-%m-%dT%H:%M:%S') # %z: removed time zone
    lat_lon_string = (
            (f'{min(df["Lat"][:]):0.6f}' + ('N' if min(df["Lat"][:]) >= 0 else 'S') + ' '
             + f'{min(df["Lon"][:]):0.6f}' + ('E' if min(df["Lon"][:]) >= 0 else 'W')) + ', '
            + f'{max(df["Lat"][:]):0.6f}' + ('N' if max(df["Lat"][:]) >= 0 else 'S') + ' '
            + f'{max(df["Lon"][:]):0.6f}' + ('E' if max(df["Lon"][:]) >= 0 else 'W')
            )
    sampling_interval = int((sonde_time_dt[1]-sonde_time_dt[0]).seconds)

    # Open NetCDF file
    dataset_out = nc.Dataset(netcdf_dir+nc_filename, 'w', format='NETCDF4_CLASSIC')
    print(netcdf_dir+nc_filename)

    # Set up dimensions
    time_dim = dataset_out.createDimension('time', len(sonde_time_dt))

    # Make sure units are right
    temp_k = pl.Series([i + 273.15 for i in list(df["Temp"])])
    df = df.with_columns(temp_k.alias("TempK"))

    # Reading is missing elapsed time, so it's recreated here.
    if radiosonde_metadata['Station name'] == 'Reading':
        elapsed_time = pl.Series(float((i - sonde_time_dt[0]).seconds) for i in list(sonde_time_dt))
        df = df.with_columns(elapsed_time.alias("Elapsed time"))

    # Set source
    if radiosonde_metadata['Station name'] == 'AshFarm':
        data_source = 'NCAS Vaisala Sounding Station unit 1'
    else:
        data_source = 'Vaisala MW41 sounding system'

    # Global attributes
    dataset_out.Conventions = 'CF-1.6, NCAS-AMF-2.0.0'
    dataset_out.source = data_source
    dataset_out.instrument_manufacturer = 'Vaisala'
    dataset_out.instrument_model = (radiosonde_metadata["Sonde type"]
                                    + ' (software v' + radiosonde_metadata["Sonde software version"] + ')'
                                    )
    dataset_out.instrument_serial_number = radiosonde_metadata["Sonde serial number"]
    dataset_out.instrument_software = radiosonde_metadata["Software version"].split(' ')[0]
    dataset_out.instrument_software_version = radiosonde_metadata["Software version"].split(' ')[1]
    dataset_out.creator_name = 'Dr Hugo Ricketts'
    dataset_out.creator_email = 'hugo.ricketts@ncas.ac.uk'
    dataset_out.creator_url = 'https://orcid.org/0000-0002-1708-2431'
    dataset_out.institution = 'National Centre for Atmospheric Science (NCAS)'
    dataset_out.processing_software_url = 'https://github.com/gapintheclouds/woest-sondes'
    dataset_out.processing_software_version = software_version_number
    dataset_out.calibration_sensitivity = 'Not Applicable'
    dataset_out.calibration_certification_date = 'N/A'
    dataset_out.calibration_certification_url = 'N/A'
    dataset_out.sampling_interval = f"{sampling_interval} {'second' if sampling_interval == 1 else 'seconds'}"
    dataset_out.averaging_interval = f"{sampling_interval} {'second' if sampling_interval == 1 else 'seconds'}"
    dataset_out.product_version = product_version_number
    dataset_out.processing_level = 1
    dataset_out.last_revised_date = current_time_string
    dataset_out.project = 'WesCon – Observing the Evolving Structures of Turbulence (WOEST)'
    dataset_out.project_principal_investigator = 'Dr Ryan Neely III'
    dataset_out.project_principal_investigator_email = 'ryan.neely@ncas.ac.uk'
    dataset_out.project_principal_investigator_url = 'https://orcid.org/0000-0003-4560-4812'
    dataset_out.licence = "".join(['Data usage licence - UK Government Open Licence agreement: ',
                                    'http://www.nationalarchives.gov.uk/doc/open-government-licence'])
    dataset_out.acknowledgement = "".join(['Acknowledgement of NCAS as the data provider is required ',
                                            'whenever and wherever these data are used'])
    dataset_out.platform = 'Launch location: ' + radiosonde_metadata["Station name"]
    dataset_out.platform_type = 'moving_platform'
    dataset_out.deployment_mode = 'trajectory'
    dataset_out.title = 'Radiosonde ascent'
    dataset_out.featureType = 'timeSeriesProfile'
    dataset_out.time_coverage_start = sonde_time_dt[0].strftime('%Y-%m-%dT%H:%M:%S')
    dataset_out.time_coverage_end = sonde_time_dt[-1].strftime('%Y-%m-%dT%H:%M:%S')
    dataset_out.geospatial_bounds = lat_lon_string
    dataset_out.platform_altitude = radiosonde_metadata["Release point height from sea level"]
    dataset_out.location_keywords = radiosonde_metadata["Station name"]
    dataset_out.amf_vocabularies_release = 'https://github.com/ncasuk/AMF_CVs/releases/tag/v2.0.0'
    dataset_out.history = current_time_string + ' - Initial processing. Flags not implemented yet.'
    dataset_out.comment = (f"Instrument owner: {sonde_system_info.system_owner}, "
                           f"Instrument operator: {sonde_system_info.system_operator}")

    # Set up variables
    times = dataset_out.createVariable('time', np.double, ('time',))
    times.type = 'double'
    times.dimension = 'time'
    times.units = 'seconds since 1970-01-01 00:00:00'
    times.standard_name = 'time'
    times.long_name = 'Time (seconds since 1970-01-01 00:00:00)'
    times.axis = 'T'
    times.valid_min = (sonde_time_dt[0] - dt.datetime(1970, 1, 1, 0, 0, 0)).total_seconds()
    times.valid_max = (sonde_time_dt[-1] - dt.datetime(1970, 1, 1, 0, 0, 0)).total_seconds()
    times.calendar = 'standard'

    day_of_year = dataset_out.createVariable('day_of_year', np.float32, ('time',))
    day_of_year.type = 'float'
    day_of_year.dimension = 'time'
    day_of_year.units = '1'
    day_of_year.standard_name = ''
    day_of_year.long_name = 'Day of Year'
    day_of_year.valid_min = sonde_time_dt[0].timetuple().tm_yday
    day_of_year.valid_max = sonde_time_dt[-1].timetuple().tm_yday

    year = dataset_out.createVariable('year', np.int32, ('time',))
    year.type = 'int'
    year.dimension = 'time'
    year.units = '1'
    year.standard_name = ''
    year.long_name = 'Year'
    year.valid_min = sonde_time_dt[0].year
    year.valid_max = sonde_time_dt[-1].year

    month = dataset_out.createVariable('month', np.int32, ('time',))
    month.type = 'int'
    month.dimension = 'time'
    month.units = '1'
    month.standard_name = ''
    month.long_name = 'Month'
    month.valid_min = 1
    month.valid_max = 12

    day = dataset_out.createVariable('day', np.int32, ('time',))
    day.type = 'int'
    day.dimension = 'time'
    day.units = '1'
    day.standard_name = ''
    day.long_name = 'Day'
    day.valid_min = 1
    day.valid_max = 31

    hour = dataset_out.createVariable('hour', np.int32, ('time',))
    hour.type = 'int'
    hour.dimension = 'time'
    hour.units = '1'
    hour.standard_name = ''
    hour.long_name = 'Hour'
    hour.valid_min = 0
    hour.valid_max = 23

    minute = dataset_out.createVariable('minute', np.int32, ('time',))
    minute.type = 'int'
    minute.dimension = 'time'
    minute.units = '1'
    minute.standard_name = ''
    minute.long_name = 'Minute'
    minute.valid_min = 0
    minute.valid_max = 59

    second = dataset_out.createVariable('second', np.float32, ('time',))
    second.type = 'float'
    second.dimension = 'time'
    second.units = '1'
    second.standard_name = ''
    second.long_name = 'Second'
    second.valid_min = 0
    second.valid_max = 59.99999

    altitudes = dataset_out.createVariable('altitude', np.float32, 'time', fill_value=this_fill_value)
    altitudes.type = 'float'
    altitudes.dimension = 'time'
    altitudes.units = 'm'
    altitudes.standard_name = 'altitude'
    altitudes.long_name = 'Geometric height above geoid (WGS 84).'
    altitudes.axis = 'Z'
    altitudes.valid_min = min(df["GpsHeightMSL"][:])
    altitudes.valid_max = max(df["GpsHeightMSL"][:])
    altitudes.cell_methods = 'time: point'

    latitudes = dataset_out.createVariable('latitude', np.float32, ('time',), fill_value=this_fill_value)
    latitudes.type = 'float'
    latitudes.dimension = 'time'
    latitudes.units = 'degrees_north'
    latitudes.standard_name = 'latitude'
    latitudes.long_name = 'Latitude'
    latitudes.axis = 'Y'
    latitudes.valid_min = min(df["Lat"][:])
    latitudes.valid_max = max(df["Lat"][:])
    latitudes.cell_methods = 'time: point'

    longitudes = dataset_out.createVariable('longitude', np.float32, ('time',), fill_value=this_fill_value)
    longitudes.type = 'float'
    longitudes.dimension = 'time'
    longitudes.units = 'degrees_east'
    longitudes.standard_name = 'longitude'
    longitudes.long_name = 'Longitude'
    longitudes.axis = 'X'
    longitudes.valid_min = min(df["Lon"][:])
    longitudes.valid_max = max(df["Lon"][:])
    longitudes.cell_methods = 'time: point'

    air_pressures = dataset_out.createVariable('air_pressure', np.float32, ('time',), fill_value=this_fill_value)
    air_pressures.type = 'float'
    air_pressures.dimension = 'time'
    air_pressures.units = 'hPa'
    air_pressures.standard_name = 'air_pressure'
    air_pressures.long_name = 'Air Pressure'
    air_pressures.valid_min = min(df["P"][:])
    air_pressures.valid_max = max(df["P"][:])
    air_pressures.cell_methods = 'time: point'
    air_pressures.coordinates = 'latitude longitude altitude'

    air_temperatures = dataset_out.createVariable('air_temperature', np.float32, ('time',), fill_value=this_fill_value)
    air_temperatures.type = 'float'
    air_temperatures.dimension = 'time'
    air_temperatures.units = 'K'
    air_temperatures.standard_name = 'air_temperature'
    air_temperatures.long_name = 'AirTemperature'
    air_temperatures.valid_min = min(df["TempK"][:])
    air_temperatures.valid_max = max(df["TempK"][:])
    air_temperatures.cell_methods = 'time: point'
    air_temperatures.coordinates = 'latitude longitude altitude'

    relative_humiditys = dataset_out.createVariable('relative_humidity', np.float32, ('time',),
                                                    fill_value=this_fill_value)
    relative_humiditys.type = 'float'
    relative_humiditys.dimension = 'time'
    relative_humiditys.units = '%'
    relative_humiditys.standard_name = 'relative_humidity'
    relative_humiditys.long_name = 'Relative Humidity'
    relative_humiditys.valid_min = min(df["RH"][:])
    relative_humiditys.valid_max = max(df["RH"][:])
    relative_humiditys.cell_methods = 'time: point'
    relative_humiditys.coordinates = 'latitude longitude altitude'

    wind_speeds = dataset_out.createVariable('wind_speed', np.float32, ('time',), fill_value=this_fill_value)
    wind_speeds.type = 'float'
    wind_speeds.dimension = 'time'
    wind_speeds.units = 'm s-1'
    wind_speeds.standard_name = 'wind_speed'
    wind_speeds.long_name = 'Wind Speed'
    wind_speeds.valid_min = min(df["Speed"][:])
    wind_speeds.valid_max = max(df["Speed"][:])
    wind_speeds.cell_methods = 'time: point'
    wind_speeds.coordinates = 'latitude longitude altitude'

    wind_from_directions = dataset_out.createVariable('wind_from_direction', np.float32, ('time',),
                                                      fill_value=this_fill_value)
    wind_from_directions.type = 'float'
    wind_from_directions.dimension = 'time'
    wind_from_directions.units = 'degree'
    wind_from_directions.standard_name = 'wind_from_direction'
    wind_from_directions.long_name = 'Wind From Direction'
    wind_from_directions.valid_min = min(df["Dir"][:])
    wind_from_directions.valid_max = max(df["Dir"][:])
    wind_from_directions.cell_methods = 'time: point'
    wind_from_directions.coordinates = 'latitude longitude altitude'

    upward_balloon_velocitys = dataset_out.createVariable('upward_balloon_velocity', np.float32, ('time',),
                                                          fill_value=this_fill_value)
    upward_balloon_velocitys.type = 'float'
    upward_balloon_velocitys.dimension = 'time'
    upward_balloon_velocitys.units = 'm s-1'
    upward_balloon_velocitys.standard_name = ''
    upward_balloon_velocitys.long_name = 'Balloon Ascent Rate'
    upward_balloon_velocitys.valid_min = min(df["AscRate"][:])
    upward_balloon_velocitys.valid_max = max(df["AscRate"][:])
    upward_balloon_velocitys.cell_methods = 'time: point'
    upward_balloon_velocitys.coordinates = 'latitude longitude altitude'

    elapsed_times = dataset_out.createVariable('elapsed_time', np.float32, ('time',), fill_value=this_fill_value)
    elapsed_times.type = 'float'
    elapsed_times.dimension = 'time'
    elapsed_times.units = 's'
    elapsed_times.standard_name = ''
    elapsed_times.long_name = 'Elapsed Time'
    elapsed_times.valid_min = min(df["Elapsed time"][:])
    elapsed_times.valid_max = max(df["Elapsed time"][:])

    # qc_flags = dataset_out.createVariable('qc_flag', np.byte, ('time',), fill_value=this_fill_value)
    # qc_flags.type = 'byte'
    # qc_flags.dimension = 'time'
    # qc_flags.units = '1'
    # qc_flags.standard_name = ''
    # qc_flags.long_name = 'Data Quality flag'
    # qc_flags.flag_values = '0b,1b,2b,3b'
    # qc_flags.flag_meanings = ('not_used\n' +
    #                           'good_data\n' +
    #                           'suspect_data_no_measurable_ascent_rate\n' +
    #                           'suspect_data_horizontal_wind_speed_equals_0_m_s-1\n'
    #                           )

    # replace NaNs with the fill value (done after min and max operations to avoid minimum reading fill value)
    df = df.fill_nan(this_fill_value)

    # Write data
    dataset_out['time'][:] = nc.date2num(sonde_time_dt, dataset_out['time'].units)
    dataset_out['altitude'][:] = df["GpsHeightMSL"][:]
    dataset_out['latitude'][:] = df["Lat"][:]
    dataset_out['longitude'][:] = df["Lon"][:]
    dataset_out['air_pressure'][:] = df["P"][:]
    dataset_out['air_temperature'][:] = df["TempK"][:]
    dataset_out['relative_humidity'][:] = df["RH"][:]
    dataset_out['wind_speed'][:] = df["Speed"][:]
    dataset_out['wind_from_direction'][:] = df["Dir"][:]
    dataset_out['upward_balloon_velocity'][:] = df["AscRate"][:]
    dataset_out['elapsed_time'][:] = df["Elapsed time"][:]
    # NOTE: Check first altitudes! Look wrong...

    # Close NetCDF file
    dataset_out.close()


def convert_sondes_to_netcdf(raw_dir, netcdf_dir):
    # stations = ['Ash_Farm', 'Castle_Cary', 'Chilbolton', 'Larkhill', 'Netheravon', 'Reading', 'Spire_View']
    stations = ['Ash_Farm', 'Chilbolton', 'Larkhill', 'Reading', 'Spire_View']  # edited list
    file_search = 'edt1sdataforv217*.txt'

    for station in stations:
        current_search = raw_dir + station + '/' + file_search
        edt_file_list = sorted(glob.glob(current_search))

        # current_edt_file = edt_file_list[0]
        for current_edt_file in edt_file_list:
            print(current_edt_file)
            df, radiosonde_metadata, data_units = do_radiosondes(current_edt_file, netcdf_dir)
            save_netcdf_file(df, radiosonde_metadata, netcdf_dir)


if __name__ == "__main__":
    import sys

    in_dir = sys.argv[1]
    out_dir = sys.argv[2]
    convert_sondes_to_netcdf(in_dir, out_dir)
