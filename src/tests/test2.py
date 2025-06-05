import xarray as xr
import fsspec
import numpy as np
import s3fs
import zarr

import dask
from dask.distributed import Client
from prov_tracking import ProvTracker

def _id(x):
  return x

def inc(i):
  return i + 1

def add(a, b):
  c = []
  for i in a[0]:
    c.append(i + b)
  return c

if __name__ == "__main__":
  client = Client()
  plugin = ProvTracker(destination = 'prov2.json', format = 'json', indent = 2, rich_types = True)
  client.register_plugin(plugin) 
  plugin.start(client.scheduler)

  x = client.submit(_id, 1)
  y = client.submit(inc, x)
  z = client.submit(add, [[y, x]], 10)
  print(z.result())

  client.close()
