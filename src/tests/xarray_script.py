from dask.distributed import Client
import xarray as xr

if __name__ == "__main__":
  client = Client(n_workers=2, threads_per_worker=2, memory_limit='8GB')

  from prov_tracking.plugin import ProvTracker

  plugin = ProvTracker(destination = 'prov_script.json', format = 'json', indent = 2, rich_types=True)
  client.register_plugin(plugin)
  plugin.start(client.cluster.scheduler)

  ds = xr.tutorial.open_dataset('air_temperature',
                                chunks={'lat': 25, 'lon': 25, 'time': -1})
  ds
  da = ds['air']
  da
  da.data
  da2 = da.groupby('time.month').mean('time')
  da3 = da - da2
  da3
  computed_da = da3.compute()
  type(computed_da.data)

  computed_da
  da = da.persist()

  resampled_da = da.resample(time='1w').mean('time')
  resampled_da.std('time')

  resampled_da.std('time').plot(figsize=(12, 8))

  da_smooth = da.rolling(time=30).mean()
  da_smooth

  da.sel(time='2013-01-01T18:00:00')
  da.sel(time='2013-01-01T18:00:00').load()

  import numpy as np
  import xarray as xr
  import bottleneck

  def covariance_gufunc(x, y):
    return ((x - x.mean(axis=-1, keepdims=True))
              * (y - y.mean(axis=-1, keepdims=True))).mean(axis=-1)

  def pearson_correlation_gufunc(x, y):
    return covariance_gufunc(x, y) / (x.std(axis=-1) * y.std(axis=-1))

  def spearman_correlation_gufunc(x, y):
    x_ranks = bottleneck.rankdata(x, axis=-1)
    y_ranks = bottleneck.rankdata(y, axis=-1)
    return pearson_correlation_gufunc(x_ranks, y_ranks)

  def spearman_correlation(x, y, dim):
    return xr.apply_ufunc(
      spearman_correlation_gufunc, x, y,
      input_core_dims=[[dim], [dim]],
      dask='parallelized',
      output_dtypes=[float])

  corr = spearman_correlation(da.chunk({'time': -1}),
                              da_smooth.chunk({'time': -1}),
                              'time')
  corr

  corr.plot(figsize=(12, 8))
