# Load required libraries
import pandas as pd
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
#3 parallel_overlay() 


def write_log(log_path, log_entry):
    # Check if the file exists to determine if headers need to be written
    file_exists = os.path.isfile(log_path)

    # Open the log file in append mode
    with open(log_path, 'a', newline='') as csvfile:
        log_writer = csv.writer(csvfile)

        # Write the header if the file is new
        if not file_exists:
            log_writer.writerow(['Timestamp', 'HDENS_NAME', 'agglo_Id', 'uc_km2', 'gdf2_km2', 'gdf1_gdf2_km2', 
                                 'ua_km2', 'uagreen_km2', 'uagreen_urbc_km2', 'nqgreen_m2', 'green_not_covered_by_gdf1_m2',
                                  'GQA_m2', 'GNA_m2', 'prDuration'])

        # Write the log entry
        log_writer.writerow(log_entry)

def create_log_entry(val1, val2, val3, val4, val5, val6, val7, val8, val9, val10, val11, val12, processing_time):
    timestamp = datetime.now().strftime('%Y%m%d_%Hh%Mm%Ss')
    processing_duration = str(processing_time)
    return [timestamp, val1, val2, val3, val4, val5, val6, val7, val8, val9, val10, val11, val12, processing_duration]

def parallel_overlay(gdf1, gdf2, npartitions=10, how='intersection'):
    """
    Perform spatial overlay (intersection) using GeoPandas in parallel with Dask.
    
    Parameters:
    gdf1 (GeoDataFrame): The first GeoDataFrame to overlay.
    gdf2 (GeoDataFrame): The second GeoDataFrame to overlay.
    npartitions (int): Number of partitions to use for Dask.
    how (str): The type of overlay operation. Default is 'intersection'.
    
    Returns:
    GeoDataFrame: The result of the overlay operation.
    """
    # Convert GeoDataFrames to Dask GeoDataFrames
    gdf1_dg = dg.from_geopandas(gdf1, npartitions=npartitions)
    gdf2_dg = dg.from_geopandas(gdf2, npartitions=npartitions)

    # Perform spatial overlay (intersection) using GeoPandas in parallel with Dask
    gdf1_parts = [gdf1_dg.partitions[i].compute() for i in range(gdf1_dg.npartitions)]
    gdf2_parts = [gdf2_dg.partitions[i].compute() for i in range(gdf2_dg.npartitions)]

    # Manually overlay using GeoPandas
    overlays = [gpd.overlay(gdf1_part, gdf2_part, how=how) for gdf1_part in gdf1_parts for gdf2_part in gdf2_parts]
    result = gpd.GeoDataFrame(pd.concat(overlays, ignore_index=True))
    
    return result    