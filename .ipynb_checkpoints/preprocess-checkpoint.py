import pandas as pd
import numpy as np
import os
import logging
import esmvalcore.preprocessor
import glob
import warnings
#warnings.filterwarnings("ignore")
import xarray as xr
from xmip.preprocessing import rename_cmip6
import matplotlib.pyplot as plt
from tqdm import tqdm
import dask
import statsmodels.api as sm


def read_in(dir, t_bnds=None, t_bnds_i=None, lat_bnds=None, months=None, last_years=None):
    files = []
    for x in os.listdir(dir): 
        files.append(dir + x)
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        ds = rename_cmip6(xr.open_mfdataset(files, parallel=True, chunks={"time": 50}))
    #ds = rename_cmip6(xr.open_mfdataset(files))
    if t_bnds:
        ds = ds.sel(time=slice(t_bnds[0], t_bnds[1]))
    if months:
        ds = ds.where((ds['time.month'].isin(months)), drop=True)
    if lat_bnds:
        ds = ds.sel(y=slice(lat_bnds[0],lat_bnds[1]))
    if last_years:
        ds = ds.isel(time=slice(-360*last_years,-1))
    if t_bnds_i:
        ds = ds.isel(time=slice(360*t_bnds_i[0],360*t_bnds_i[1]))
    
    #print(dir, ds['time.year'].min().values) #check that only selecting specific time files has worked ok
    return ds


def remove_seasonal_trend(ds):
    return ds.groupby('time.dayofyear') - ds.groupby('time.dayofyear').mean('time')

def remove_linear_trend(ds):
    
    data_detrended = ds.apply(detrend)
    
    out = ds
    return out

# Function to detrend
# Source: https://gist.github.com/rabernat/1ea82bb067c3273a6166d1b1f77d490f
def detrend_dim(ds, dim='time', deg=1, var='tas'):
    """detrend along a single dimension."""
    # calculate polynomial coefficients
    da = ds[var]
    p = da.polyfit(dim=dim, deg=deg, skipna=False)
    # evaluate trend
    fit = xr.polyval(da[dim], p.polyfit_coefficients)
    out = da-fit
    # remove the trend
    return out.to_dataset(name=var)


control_path = '/badc/cmip6/data/CMIP6/CMIP/MOHC/UKESM1-0-LL/piControl/r1i1p1f2/day/tas/gn/latest/'
G1_path_1 = '/badc/cmip6/data/CMIP6/GeoMIP/MOHC/UKESM1-0-LL/G1/r1i1p1f2/day/tas/gn/latest/'
G1_path_2 = '/badc/cmip6/data/CMIP6/GeoMIP/MOHC/UKESM1-0-LL/G1/r2i2p1f2/day/tas/gn/latest/'
G1_path_3 = '/badc/cmip6/data/CMIP6/GeoMIP/MOHC/UKESM1-0-LL/G1/r3i2p1f2/day/tas/gn/latest/'
CO2_4x_path = '/badc/cmip6/data/CMIP6/CMIP/MOHC/UKESM1-0-LL/abrupt-4xCO2/r1i1p1f2/day/tas/gn/latest/'



paths = [control_path, G1_path_1, G1_path_2, G1_path_3, CO2_4x_path]

scenarios = ['piControl', 'G1', 'G1', 'G1', 'abrupt-4xCO2']
experiments = ['piControl', 'G1_1', 'G1_2', 'G1_3', 'abrupt-4xCO2']

lat_bs = [-23, 23]

# 
t_bnds_long = [[-20, -1], [-40, -21], [-60, -41], [-80, -61], [-100, -81]]
t_bnds_short = [[-20, -1], [-40, -21]]

t_bnds_dict = {'piControl': t_bnds_long,
               'G1': t_bnds_short,
               'abrupt-4xCO2': t_bnds_long}


lags = np.arange(0, 51, 1)


DF = pd.DataFrame(columns=['lag', 'acorr', 'lon', 'lat', 'scenario'])
s=0
for path in paths:
    scenario = scenarios[s]
    experiment = experiments[s]
    print(scenario, ' ', experiment)
    for t_bnds_it in t_bnds_dict[scenario]:
        print(str(t_bnds_it[0]))
        ds = read_in(control_path, lat_bnds = lat_bs, t_bnds_i=t_bnds_it)
        ds = remove_seasonal_trend(ds)
        ds = detrend_dim(ds)
        ds.load()
        lons, lats = ds.x.values, ds.y.values
        iter=0
        for lon in tqdm(lons):
            for lat in lats:
                ts = ds.sel(x=lon, y=lat).tas.values
                acorr = sm.tsa.acf(ts, nlags = len(lags)-1)
                df = pd.DataFrame({'lag': lags,
                                   'acorr': acorr})
                df['lon'] = lon
                df['lat'] = lat
                df['iter'] = iter
                df['scenario'] = scenario
                df['experiment'] = experiment
                
                DF = pd.concat([DF, df])
                iter=iter+1
        
        DF.to_csv('temperature_autocorrelations/{f}/autocorrelations_{lat1}_{lat2}_{t1}_{t2}.csv'.format(
                f=scenario,
                lat1=str(lat_bs[0]), lat2=str(lat_bs[1]), 
                t1=t_bnds_it[0], t2=t_bnds_it[1]))
        ds.close()
        print(experiment, str(t_bnds_it[0]), ' done')
    s=s+1