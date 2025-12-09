

import os
import csv
import pandas as pd
import numpy as np
from glob import glob
import shutil
import psycopg2

from sqlalchemy import create_engine

from osgeo import gdal, ogr, osr


import rasterio
from rasterio.plot import show
import geopandas as gpd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import subprocess

from sqlalchemy import create_engine

from ..processing.tools import *

from glob import glob


ScheMa = postgis_cfg['schema']


DirInScr = config_dict['path']['fastio_dir']


# Connect to PostGIS
ConnectInfo = PgId()

postgis_engine = create_engine('postgresql://'+postgis_user+':'+postgis_password+'@'+\
                    postgis_host+':'+str(postgis_port)+'/'+postgis_dbname+'')

## refocus_db
postgis_host     = "0000"
postgis_port     = '0000'
postgis_dbname   = "0000"
postgis_user     = "0000"   # ""
postgis_password = "0000"          # ""

List = list(range(2023,2008-1,-1))
List = list(range(2008,2017+1))
List = [2023]


# conn_string = (    f"PG:"+ConnectInfo)
ConnectInfo= "dbname='"+postgis_dbname+"' port='"+str(postgis_port)+"' user='"+postgis_user+"' host='"+postgis_host+"' password='"+postgis_password+"'"

conn_string = (    f"PG:"+ConnectInfo)

pixel_size = 10000
x_min, y_min, x_max, y_max = 2640000, 1560000 ,5810000, 5310000

# Calculate the number of pixels based on the extent and pixel size
cols = int((x_max - x_min) / pixel_size)
rows = int((y_max - y_min) / pixel_size)

output_projection = "EPSG:3035"

root = '/eos/jeodpp/data/projects/REFOCUS/clamart/data/cheap/GSA_Grids/'
# root = '/eos/jeodpp/data/projects/REFOCUS/clamart/data/cheap/'

DicSelCrops = {
    3310000000:'arable_crops',
    3310100000:'cereal',
    3310101000:'common_soft_wheat',
    3310102000:'durum_hard_wheat',
    3310103000:'rye',
    3310104000:'barley',
    3310106000:'maize_corn_popcorn',
    3310107000:'rice',
    3310108000:'triticale',
    3310116000:'sorghum',
    3310200000:'legumes',
    3310202010:'alfalfa_lucerne',
    3310300000:'potatoes',
    3310604000:'oilseed_crops',
    3310604010:'rapeseed_rape',
    3310604020:'sunflower',
    3310700000:'fresh_vegetables',
    3310719010:'sugar_beet',
    3310800000:'flowers_ornamental_plants',
    3320000000:'grassland_grass',
    3320100000:'permanent_grassland',
    3320200000:'temporary_grassland',
    3330000000:'permanent_crops_perennial',
    3330100000:'orchards_fruits',
    3330300000:'nuts',
    3330400000:'citrus',
    3330500000:'olive',
    3330600000:'vineyards_wine_vine_rebland_grapes',
    3350000000:'greenhouse_foil_film',
    3360000000:'tree_wood_forest',
    3370000000:'unmaintained',
    3390000000:'not_known_and_other'}


sql = 'select LEAST(255,round(255*area_ha*10000/10000/10000)::INT) area_perc,geom from grid.grid_gsa_YeaR_CroP'

for Y in List:
    print(Y)
    for c in DicSelCrops.keys():
        cropname = DicSelCrops[c]
        hcat = str(c)

        output_raster_path = root + 'CropGrid.'+str(Y)+'.'+hcat+'.'+cropname+'.tif'
        
        sql_query = sql.replace('YeaR', str(Y)).replace('CroP', DicSelCrops[c])
        # import pdb ; pdb.set_trace()
        raster_ds = gdal.GetDriverByName('GTiff').Create(output_raster_path, cols, rows, 1, gdal.GDT_Byte)
        raster_ds.SetGeoTransform((x_min, pixel_size, 0, y_max, 0, -pixel_size))
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(3035)  # Replace with your projection's EPSG code
        raster_ds.SetProjection(srs.ExportToWkt())
        band = raster_ds.GetRasterBand(1)
        band.SetNoDataValue(0)

        # Open the PostGIS layer
        vector_ds = ogr.Open(conn_string)
        vector_layer = vector_ds.ExecuteSQL(sql_query)

        # Rasterize the PostGIS layer
        gdal.RasterizeLayer(raster_ds, [1], vector_layer, options=["ATTRIBUTE=area_perc"])

        # Set NoData value (optional)
        band = raster_ds.GetRasterBand(1)
        band.SetNoDataValue(0)

        # Close the datasets
        raster_ds = None
        vector_ds = None

        # Query PostGIS polygons into a GeoDataFrame
        gdf = gpd.read_postgis('SELECT wkb_geometry FROM out.admin_'+str(Y), con=postgis_engine, geom_col='wkb_geometry')

        # Load GeoTIFF data
        with rasterio.open(output_raster_path) as src:
            tiff_data = src.read(1)  # Read the first band
            tiff_transform = src.transform
            tiff_bounds = src.bounds  # Get GeoTIFF bounds
        # import pdb ; pdb.set_trace()

        # Plotting
        fig, ax = plt.subplots(figsize=(10, 10))

        # Plot the GeoTIFF
        tiff_image = ax.imshow(tiff_data, cmap='viridis', 
                extent=(tiff_bounds.left, tiff_bounds.right, tiff_bounds.bottom, tiff_bounds.top))
        
        # , vmin=0, vmax=50
        cbar = plt.colorbar(tiff_image, ax=ax, orientation='vertical')
        cbar.set_label('Values')

        # Plot the PostGIS polygons
        gdf.boundary.plot(ax=ax, edgecolor='lightgrey', linewidth=0.5)


        ax.set_xlim([tiff_bounds.left, tiff_bounds.right])
        ax.set_ylim([tiff_bounds.bottom, tiff_bounds.top])

        # plt.ylim(yl)
        # plt.xlim(xl)
        # Set plot title and labels
        ax.set_title(' - '.join(output_raster_path.split('/')[-1].split('.')[1:-1]))
        # ax.set_xlabel('Longitude')
        # ax.set_ylabel('Latitude')

        # Save the map as a PNG file
        output_path = output_raster_path.replace('.tif','.png').replace('CropGrid','CropGridMap')
        plt.savefig(output_path, dpi=300)
        plt.close()

        
# for Y in List:
    # composite layer
    txt_inputs = '' 
    for cn in ['permanent_crops_perennial','grassland_grass','arable_crops']: # R G B
        dum = glob(root + 'CropGrid.'+str(Y)+'.*.'+cn+'.tif')
        txt_inputs += dum[0] + ' '


    output_raster_compo = root + 'CropGrid.'+str(Y)+'.composite.vrt'
    RunSysCommand('gdalbuildvrt -overwrite -resolution average -separate -r nearest -srcnodata 256 '+output_raster_compo+' ' +txt_inputs)
