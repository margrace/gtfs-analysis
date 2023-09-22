import os
import io
import sys
import zipfile
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

#%% FUNCTIONS

def load_tables(dataset:str):

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

#%% TEST

dataset = 'EMT_VLC.zip'
gtfs = load_tables(dataset)
gtfs_test = {'stops':0, 'stop_times':1, 'trips':2, 'routes':3}

check_tables(gtfs)