from dask.distributed import Client
from prov_tracking import ProvTracker

def inc(a):
  return a + 1

def add(a, b):
  return a + b

if __name__ == "__main__":
  client = Client()
  plugin = ProvTracker(
    name = 'test2', destination = './output',
    keep_traceback=True, rich_types=True
  )
  client.register_plugin(plugin) 
  plugin.start(client.scheduler)

  x = client.submit(inc, 1)
  y = client.submit(inc, 2)
  z = client.submit(add, x, y)
  print(z.result())

  client.close()
