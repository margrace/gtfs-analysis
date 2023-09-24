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
                df = pd.read_csv(io.StringIO(text_data), delimiter=',')
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

def get_interstop_speed(gtfs:dict, date:str, format:str='%Y%m%d'):

    """
    Returns the average speed between stations for every route within a particular day.

    inputs: gtfs(dict) -> pandas DataFrame containing all of the fetched tables from the GTFS zipfile.
            date(str) -> date that the average speed wants to be checked on.

    output: avg_speed(dict)-> dictionary containing the average speed for every route. 
    
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

    if not vali_date(date=date, format=format):
        raise ValueError(f"Incorrect date format, you specified {format}")
        return False
    
    date = dt.strptime(date, format).strftime('%Y%m%d')

    if 'calendar_dates' in gtfs.keys():
        cal_dates = gtfs['calendar_dates'].loc[(gtfs['calendar_dates'].date==int(date)),:]
        exc_type = cal_dates.exception_type.values
        print(cal_dates)
        print(exc_type)

        if len(cal_dates) == 0:
            f = 0
        elif 1 not in exc_type:
            raise ValueError("The date you selected has no transit service available in the provided GTFS data.")
            return False
        else:
            service_id = cal_dates.loc[cal_dates.exception_type==1,'service_id'].values
        
    else:
        dtdate = dt.strptime(date, format)
        dow = dow_dict[dtdate.weekday()]
        cal = gtfs['calendar'].copy()
        cal['start_date'] = pd.to_datetime(arg = cal.start_date, format='%Y%m%d')
        cal['end_date'] = pd.to_datetime(arg = cal.end_date, format='%Y%m%d')
        service_id = cal[(cal.start_date <= dtdate) & (cal.end_date >= dtdate) & (cal[dow]==1), 'service_id'].values

    return service_id
    



#%% TEST

dataset = 'EMT_VLC.zip'
gtfs = load_tables(dataset)
gtfs_test = {'stops':0, 'stop_times':1, 'trips':2, 'routes':3}

check_tables(gtfs)
cds = gtfs['calendar_dates']
#print(cds.loc[cds.date==20230417,:])
print(get_interstop_speed(gtfs, '20230217'))