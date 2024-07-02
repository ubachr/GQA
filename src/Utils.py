# Load required libraries
import geopandas as gpd
import numpy as np
import os
import fiona
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from shapely.ops import unary_union
from unidecode import unidecode
import glob
import csv
from datetime import datetime
import dask.dataframe as dd
import dask_geopandas as dg
from dask.distributed import Client


# List of defined functions:
#1 write_log(log_path, log_entry)
#2 create_log_entry()


def write_log(log_path, log_entry):
    # Check if the file exists to determine if headers need to be written
    file_exists = os.path.isfile(log_path)

    # Open the log file in append mode
    with open(log_path, 'a', newline='') as csvfile:
        log_writer = csv.writer(csvfile)

        # Write the header if the file is new
        if not file_exists:
            log_writer.writerow(['Timestamp', 'HDENS_NAME', 'agglo_Id', 'uc_km2', 'agl_city_km2', 'ncm_agl_city_km2', 
                                 'ua_km2', 'uagreen_km2', 'uagreen_urbc_km2', 'nqgreen_m2', 'green_not_covered_by_ncm_m2',
                                  'GQA_m2', 'GNA_m2', 'prDuration'])

        # Write the log entry
        log_writer.writerow(log_entry)

def create_log_entry(val1, val2, val3, val4, val5, val6, val7, val8, val9, val10, val11, val12, processing_time):
    timestamp = datetime.now().strftime('%Y%m%d_%Hh%Mm%Ss')
    processing_duration = str(processing_time)
    return [timestamp, val1, val2, val3, val4, val5, val6, val7, val8, val9, val10, val11, val12, processing_duration]
