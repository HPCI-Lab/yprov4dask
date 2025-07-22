from dask.distributed import Client
from prov_tracking import ProvTracker

def inc(a):
  return a + 1

def add(a, b):
  return a + b

def square(a):
  return a * a

if __name__ == "__main__":
  client = Client()
  plugin = ProvTracker(
    name = 'super', destination = './output',
    keep_traceback=True, rich_types=True
  )
  client.register_plugin(plugin) 
  plugin.start(client.scheduler)

  x = client.submit(inc, 1)
  y = client.submit(square, x)
  z = client.submit(add, x, y)
  print(z.result())

  client.close()
