import sys, os, importlib, json
import rasterio

import pandas as pd
import geopandas as gpd

from shapely.geometry import Point, LineString
from centerline.geometry import Centerline

json_folder = os.path.dirname(__file__)

class ais_file(object):
    """ Define and process an ais_file
    """
    
    def __init__(self, ais_file):
        self.ais_file = ais_file
        with open(os.path.join(json_folder, "ship_statuses.json"), 'r') as json_file:
            self.ship_status = json.load(json_file)
        with open(os.path.join(json_folder, "ship_types.json"), 'r') as json_file:
            self.ship_types = json.load(json_file)               
        xx = {}
        for key, item in self.ship_status.items():
            xx[int(key)] = item
        self.ship_status = xx               
        xx = {}
        for key, item in self.ship_types.items():
            for i in item:
                xx[i] = key
        self.ship_types = xx
        
    def read_simple_geom(self, convert_cols=True, good_cols = ['latitude','longitude','timestamp','mmsi','ship_and_cargo_type','status']):
        ''' Read in the ais file, convert to geopandas dataframe
        
        Args:
            convert_cols (boolean, optional): option to convert columns ship_and_cargo_type and status to text, defaults to True
            good_cols(list of strings, optional): columns to keep when returning results
            
        Returns:
            geopandas GeoDataFrame: ais_file cleaned according to arguments.
        '''
        
        inD = pd.read_csv(self.ais_file)
        self.rawD = inD
        inD = inD.loc[:,good_cols]
        if convert_cols:
            inD['ship_type'] = inD['ship_and_cargo_type'].replace(self.ship_types)
            inD['status_name'] = inD['status'].replace(self.ship_status)
            
        inD_geom = [Point(x.longitude, x.latitude) for idx, x in inD.iterrows()]
        inD = gpd.GeoDataFrame(inD, geometry=inD_geom, crs="epsg:4326")
        self.gdf = inD
        return(inD)
        
def generate_linear_features(inD, simplify_thresh=0.001):
    ''' Convert point locations from AIS data into linear features
    '''
    inD['DAY'] = inD['timestamp'].apply(lambda x: x[:10])
    inD_grp = inD.groupby(['DAY'])
    all_shps = []
    for idx, group in inD_grp:
        group.sort_values('timestamp', ascending=True, inplace=True)
        group = group.loc[~group['latitude'].isna()]     
        group = group.loc[~group.duplicated()]
        if group.shape[0] > 1:
            if (group['longitude'].max() - group['longitude'].min()) > 300:
                # If the group crosses the anti-meridian, create separate groups based on the side of the meridian
                group = group.reset_index()
                group['DIFF'] = group['longitude'].diff()
                group['DIFF'] = abs(group['DIFF'])
                group['AM_G'] = 0
                grp_cnt = 1
                for g_idx, row in group.loc[group['DIFF'] > 300].iterrows():
                    group.loc[g_idx:group.index.max(),'AM_G'] = grp_cnt
                    grp_cnt += 1
                sel_group = group.groupby('AM_G')
                for sub_idx, sub_group in sel_group:
                    shp = LineString(sub_group['geometry'].values)
                    shp = shp.simplify(simplify_thresh)
                    all_shps.append([f'{idx}_{sub_idx}', shp])
            else:
                shp = LineString(group['geometry'].values)
                shp = shp.simplify(simplify_thresh)
                all_shps.append([idx, shp])
    
    all_lines = gpd.GeoDataFrame(pd.DataFrame(all_shps, columns=['Day','geometry']), geometry = 'geometry', crs=inD.crs)       
    return(all_lines)