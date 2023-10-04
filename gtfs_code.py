import os
import io
import sys
import zipfile
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from datetime import datetime as dt

#%% FUNCTIONS

def load_tables(dataset:str) -> dict:

    """ 
    Returns a dictionary containing all of the text files from the GTFS package converted to pandas DataFrames.

    input : dataset (str) -> the name of the zip file contained witin the data folder.
    output : gtfs (dict) -> dictionary containing all of the tables in the GTFS zip file.
    """

    root = os.path.dirname(os.path.abspath(__file__))
    gtfs = {}

    with zipfile.ZipFile(root+f'\data\{dataset}','r') as gtfs_zip:
        file_names = gtfs_zip.namelist()

        for file_name in file_names:
            with gtfs_zip.open(file_name) as file:
                file_name_without_extension = file_name.split('.')[0]
                text_data = file.read().decode('utf-8')
                df = pd.read_csv(io.StringIO(text_data), delimiter=',', dtype=str)
                gtfs[file_name_without_extension] = df.copy()
    
    return gtfs

def check_tables(gtfs:dict):

    """
    Checks if all essential GTFS tables are included in the provided dataset

    input : gtfs(dict) -> pandas DataFrame containing all of the fetched tables from the GTFS zipfile.
    """

    essential_tables = ['agency', 'calendar', 'routes', 'stop_times', 'stops', 'trips']
    present_tables = list(gtfs.keys())
    missing_tables = list(set(essential_tables)-set(present_tables))

    if len(missing_tables) == 0:
        print("All essential tables are included.")
    else:
        error_str = "The "
        for mt in missing_tables:
            error_str += f"\"{mt}\", "
        error_str = error_str[:-2] + " tables are missing, please check your GTFS zipfile."
        print(error_str)

def vali_date(date:str, format:str) -> bool:

    """
    Checks if a date string is in a specific format.

    input : date(str) -> a string containing the date that needs checking.
            format(str) -> a string containing the format.
    output : bool
    """
    try:
        if date != dt.strptime(date, format).strftime(format):
            raise ValueError
    except ValueError:
        return False
    
    return True

def get_services(gtfs:dict, date:str, format:str='%Y%m%d') -> np.array:

    """
    Returns a list of the services available for a specific day.

    input:  gtfs(dict) -> a dictionary containing all gtfs tables
            date(str) -> the date for which the available services want to be retrieved
            format(str) -> the format in which the date is parsed

    output: services(np.array) -> an array containing all of the service ids for the date.
    """
    
    dow_dict = {
        0 : 'monday',
        1 : 'tuesday',
        2 : 'wednesday',
        3 : 'thursday',
        4 : 'friday',
        5 : 'saturday',
        6 : 'sunday'
    }

    if not vali_date(date, format):
        raise ValueError('The date is not valid')
    
    dt_date = dt.strptime(date, format)
    dow = dow_dict[dt_date.weekday()]

    calendar = gtfs['calendar'].copy()
    calendar.start_date = pd.to_datetime(calendar.start_date)
    calendar.end_date = pd.to_datetime(calendar.end_date)

    conditions = ((calendar.start_date <= dt_date) &
                  (calendar.end_date >= dt_date) &
                  (calendar[dow] == '1'))

    services = calendar.loc[conditions,'service_id'].values
    
    try:
        calendar_dates = gtfs['calendar_dates'].copy()
        exceptions = calendar_dates.loc[calendar_dates.date == dt_date.strftime('%Y%m%d'),:]

        if exceptions.shape[0] > 0:
            to_add = exceptions.loc[exceptions.exception_type == '1', 'service_id'].values
            to_remove = exceptions.loc[exceptions.exception_type == '2', 'service_id'].values

            services = np.array(list(set(services) - set(to_remove) | set(to_add)))

            return services
        else:
            return services

    except:
        return services

def get_trips(gtfs:dict, date:str, format:str='%Y%m%d', routes:np.array=np.array([])) -> np.array:

    """
    Returns a numpy array of the trips planned on a specific date.

    input : gtfs(dict) -> dictionary containing the gtfs tables under pandas dataframe format.
            date(str) -> the date for which the trips we want to obtain.
            format(str) -> the format the date is parsed in.

    output : trip_id(np.array) -> a numpy array of the trips. 
    """

    services = get_services(gtfs, date, format)
    trips = gtfs['trips'].copy()

    trips = trips[trips.service_id.isin(services)]

    if routes.shape[0]>0:
        trips = trips[trips.route_id.isin(routes)]
        return trips
    else:
        return trips
    
def get_interstop_speed(gtfs:dict, date:str, format:str='%Y%m%d', routes:np.array=np.array([])) -> dict:

    """
    Returns the average speed between stations by route.

    input:  gtfs(dict) -> a dictionary containing all gtfs files
            date(str) -> the date the analysis is to be made of.
            format(str) -> the format the date is parsed in.
            routes(np.array) -> an array of the routes that want to be included (if empty, all)
    
    output: speed(dict) -> a dictionary containing the average speed for each route.
    """

    trip_ids = get_trips(gtfs, date, format, routes)    
    trips = gtfs['trips'].copy()
    stop_times = gtfs['stop_times'].copy()
    trips = trips[trips.trip_id.isin(trip_ids)]
    stop_times = stop_times[stop_times.trip_id.isin(trip_ids)]

    stop_times = pd.DataFrame()

    stop_times.stop_sequence = stop_times.stop_sequence.astype(int)
    stop_times.sort_values(by=['trip_id','stop_sequence'], inplace=True)

    stop_times['arrival_seconds'] = (stop_times.arrival_time.str.split(':',expand=True).astype(int) * np.array([3600,60,1])).sum(axis=1)
    stop_times['departure_seconds'] = (stop_times.departure_time.str.split(':',expand=True).astype(int) * np.array([3600,60,1])).sum(axis=1)

    stop_times['interstop_time'] = stop_times.arrival_seconds - stop_times.groupby('trip_id').departure_seconds.shift()







#%% TEST

dataset = 'EMT_VLC.zip'
gtfs = load_tables(dataset)
gtfs_test = {'stops':0, 'stop_times':1, 'trips':2, 'routes':3}

check_tables(gtfs)
cds = gtfs['calendar_dates']
#print(cds.loc[cds.date==20230417,:])

print(get_services(gtfs, date='20230624'))
# %%
