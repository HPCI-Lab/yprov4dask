from dask.distributed import Client
from prov_tracking import ProvTracker

def add(a, b):
  return a + b

if __name__ == "__main__":
  client = Client()
  plugin = ProvTracker(
    destination = './output',
    keep_traceback=True, rich_types=True
  )
  client.register_plugin(plugin) 
  plugin.start(client.scheduler)

  x = client.submit(add, 1, 1)
  y = client.submit(add, x, 1)
  z = client.submit(add, y, 1)
  print(z.result())

  client.close()
