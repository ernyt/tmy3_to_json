# To run this script, make sure the tmy3.csv and TMY3_StationsMeta.csv files are in the same
# directory as this script. Use the following command to run the script
# >  python tmy_to_json.py

# This script has been tested using python 3.9.0 and pandas 2.2.0

import json
import pandas as pd
import datetime as dt
import copy

# read CSV files
print("reading CSV files")

# take only columns USAF, Site Name, Latitude, Longitude, TZ in the station metadata CSV
stations_df = pd.read_csv('TMY3_StationsMeta.csv', usecols=['USAF', 'Site Name', 'Latitude', 'Longitude', 'TZ'])

# take only columns Date, GHI, DNI, station_number in tmy3.csv
tmy3_df = pd.read_csv('tmy3.csv', usecols=['Date (MM/DD/YYYY)', 'GHI (W/m^2)', 'DNI (W/m^2)', 'station_number'])
tmy3_df = tmy3_df.rename(columns={'Date (MM/DD/YYYY)':'Date'})

# Format is [8935200 rows x 69 columns] which is 1020 x 365 x 24 rows
# GHI (W/m^2), DNI (W/m^2)

# --- METHOD ---
# STAGE 1
    # from stations_df make new coordinates column from latitude and longitude that is an array of the two items
    # change column headers to "id", "site_name", "coordinates" (TZ is kept as is)
    # create json (list of dicts) from df

# STAGE 2
    # loop through each station dict
        # filter tmy3_df down this station only
        # resample new df to weeks, averaging the ghi & dni columns and lowest date value
        # convert this df to a list of dicts
        # add to station dict as "data"
    # dump the list of dicts as json into a json file



# create a coordinates column that is a 2 element list of lat & lon
stations_df['coordinates'] = stations_df.apply(lambda row: [row['Latitude'], row['Longitude']], axis=1)

# drop and rename columns to clean up the dataframe
stations_df = stations_df.drop(columns=['Latitude','Longitude'])
stations_df = stations_df.rename(columns={"USAF": "id", "Site Name": "site_name"}, errors="raise")

# convert dataframe to list of dictionaries
output_data = stations_df.to_dict("records")
output_data_same_y = copy.deepcopy(output_data)
output_data_diff_y = copy.deepcopy(output_data)

# since TMY3 data uses data from different years for each month
# I will provide two different week summaries
# 1. Has all the the data as same year
# 2. Has all the data as the dates as per the tmy3 file (i.e. may be different years per month)

# JSON 1 - same year
print("json file 1 starting")

for station_info in output_data_same_y:

    # filter to get tmy3 dataframe rows for this station
    station_tmy3_df = tmy3_df[tmy3_df['station_number'] == station_info['id']].reset_index(drop=True)

    # convert Date column to datetime for resampling later, station_number is no longer needed
    station_tmy3_df['Date'] = pd.to_datetime(station_tmy3_df['Date'])
    station_same_y_df = station_tmy3_df.copy().drop(columns=['station_number'])
    
    # change Date column to all have the same year for this station
    set_year = 1990 # arbitrary non-leap year chosen
    station_same_y_df['Date'] = station_same_y_df['Date'].apply(lambda x: x.replace(year = set_year))
    
    # resample to get the average GHI and DNI per week, Date is the start of the week
    station_same_y_df = station_same_y_df.resample("7D",on='Date').mean().reset_index()
    
    # convert date to epoch timestamp and adjust for timezone
    station_same_y_df['Date'] = (station_same_y_df['Date'] - dt.datetime(1970,1,1)).dt.total_seconds() - (station_info['TZ'] * 3600)
    
    # convert values to integers
    station_same_y_df['Date'] = station_same_y_df['Date'].astype(int) 
    station_same_y_df['GHI (W/m^2)'] = station_same_y_df['GHI (W/m^2)'].round().astype(int) 
    station_same_y_df['DNI (W/m^2)'] = station_same_y_df['DNI (W/m^2)'].round().astype(int) 
    station_same_y_df = station_same_y_df.rename(columns={'Date':'timestamp','GHI (W/m^2)':'ghi','DNI (W/m^2)':'dni'})

    # convert dataframe to list of dictionaries
    station_data = station_same_y_df.to_dict("records")
    station_info['data'] = station_data

    # clean up station_info dict
    station_info['id'] = str(station_info['id'])
    del station_info['TZ']

# write list of dicts to json file
filename = 'tmy3_weekly_avg_same_year.json'
with open(filename, 'w') as json_file:
    json.dump(output_data_same_y, json_file, indent=4)


# JSON 2 - different years
print("json file 2 starting")

for station_info in output_data_diff_y:

    # filter to get tmy3 dataframe rows for this station
    station_tmy3_df = tmy3_df[tmy3_df['station_number'] == station_info['id']].reset_index(drop=True)
    
    # convert Date column to datetime for resampling later
    station_tmy3_df['Date'] = pd.to_datetime(station_tmy3_df['Date'])
    station_diff_y_df = station_tmy3_df.copy().drop(columns=['station_number'])

    # convert date column to MM-DD-YYYY format so that it can be sorted correctly
    station_diff_y_df['Date (MM-DD-YYYY)'] = station_diff_y_df['Date'].dt.strftime('%m-%d-%Y')
    
    # resample to get the average GHI and DNI per week, Date is the start of the week
    station_diff_y_df = station_diff_y_df.groupby(station_tmy3_df.index // (24*7)).agg({'Date (MM-DD-YYYY)':'min','GHI (W/m^2)':'mean','DNI (W/m^2)':'mean'})
    
    # convert date column back to datetime format
    station_diff_y_df['Date'] = pd.to_datetime(station_diff_y_df['Date (MM-DD-YYYY)'], format='%m-%d-%Y')
    station_diff_y_df = station_diff_y_df[['Date','GHI (W/m^2)','DNI (W/m^2)']]

    # convert date to epoch timestamp and adjust for timezone
    station_diff_y_df['Date'] = (station_diff_y_df['Date'] - dt.datetime(1970,1,1)).dt.total_seconds() - (station_info['TZ'] * 3600)
    
    # convert values to integers
    station_diff_y_df['Date'] = station_diff_y_df['Date'].astype(int) 
    station_diff_y_df['GHI (W/m^2)'] = station_diff_y_df['GHI (W/m^2)'].round().astype(int) 
    station_diff_y_df['DNI (W/m^2)'] = station_diff_y_df['DNI (W/m^2)'].round().astype(int) 
    station_diff_y_df = station_diff_y_df.rename(columns={'Date':'timestamp','GHI (W/m^2)':'ghi','DNI (W/m^2)':'dni'})

    # convert dataframe to list of dictionaries
    station_data = station_diff_y_df.to_dict("records")
    station_info['data'] = station_data

    # clean up station_info dict
    station_info['id'] = str(station_info['id'])
    del station_info['TZ']


# write list of dicts to json file
filename = 'tmy3_weekly_avg_diff_years.json'
with open(filename, 'w') as json_file:
     json.dump(output_data_diff_y, json_file, indent=4)

#started at 11:21