import dask
from distributed import Client
from prov_tracking.plugin import ProvTracker
import xarray as xr
import math

def bar(x, y):
  return x - y

def foo(x, y):
  return x + bar(x, y)

def baz(a, d):
  return a * d

if __name__ == '__main__':
  client: Client = Client()
  plugin = ProvTracker(destination = 'prov.json', format = 'json', indent = 2, keep_traceback=True, rich_types=True)
  client.register_plugin(plugin)
  plugin.start(client.scheduler)

  # Submitting this function allows the plugin to track it
  ds = client.submit(xr.tutorial.open_dataset, 
    "air_temperature",
    chunks={  # this tells xarray to open the dataset as a dask array
        "lat": 25,
        "lon": 25,
        "time": -1,
    },
  ).result()
  # Computes the mean along the `time` axis. The arrays is reduced from 3D to 2D.
  # It create a 2D matrix in which, for each pair of (lat, lon), it shows the
  # average air temperature across time. Chunking is preserved, so we have chunks
  # of size (25) for lat and (25, 25, 3) for lon
  air = ds['air']
  ds_mean = ds.mean(dim = 'time')
  ds_mean = ds_mean.compute()

  x = client.submit(baz, d=3, a=6)
  z = client.submit(math.sin, x)
  
  client.close()