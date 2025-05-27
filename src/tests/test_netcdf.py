import xarray as xr
from dask.distributed import Client
from prov_tracking import ProvTracker

if __name__ == '__main__':
  client = Client()
  plugin = ProvTracker(destination = 'prov_netcdf.json', format = 'json', indent = 2, rich_types = True)
  client.register_plugin(plugin)
  plugin.start(client.scheduler)

  file = '/various/shared/università/master-thesis/python/clt_day_CanESM5_ssp126_r12i1p2f1_gn_20150101-21001231.nc'
  ds = xr.open_dataset(file, engine = 'netcdf4', chunks={
    'time': -1
  })
  print(ds)
  ds_mean = ds.mean(dim='bnds')
  ds_mean_groups = ds_mean.groupby(group='time.year')
  summed_values = ds_mean_groups.sum()
  result = summed_values.compute()

  client.close()