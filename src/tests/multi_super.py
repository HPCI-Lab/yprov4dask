from dask.distributed import Client
from prov_tracking import ProvTracker

def inc(a):
  return a + 1

def add(*args):
  return sum(args)

def square(a):
  return a * a

if __name__ == "__main__":
  client = Client()
  plugin = ProvTracker(
    name = 'multi_super', destination = './output',
    keep_traceback=True, rich_types=True
  )
  client.register_plugin(plugin) 
  plugin.start(client.scheduler)

  x1 = client.submit(inc, 1)
  y1 = client.submit(square, x1)
  x2 = client.submit(inc, 2)
  y2 = client.submit(square, x2)
  z = client.submit(add, x1, y1, x2, y2)
  print(z.result())

  client.close()
