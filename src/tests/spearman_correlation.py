from dask.distributed import Client
import xarray as xr
import bottleneck

if __name__ == "__main__":
  client = Client(n_workers=2, threads_per_worker=2, memory_limit='8GB')

  from prov_tracking.plugin import ProvTracker

  plugin = ProvTracker(
    destination = './output',
    keep_traceback=True, rich_types=True
  )
  client.register_plugin(plugin)
  plugin.start(client.cluster.scheduler)

  ds = xr.tutorial.open_dataset(
    'air_temperature',
    chunks={'lat': 25, 'lon': 25, 'time': -1}
  )
  da = ds['air']
  da = da.persist()
  da_smooth = da.rolling(time=30).mean()
  da.sel(time='2013-01-01T18:00:00').load()

  def covariance_gufunc(x, y):
    return (
      (x - x.mean(axis=-1, keepdims=True)) *
      (y - y.mean(axis=-1, keepdims=True))
    ).mean(axis=-1)

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
      output_dtypes=[float]
    )

  corr = spearman_correlation(
    da.chunk({'time': -1}), da_smooth.chunk({'time': -1}), 'time'
  )
  corr.plot(figsize=(12, 8))
