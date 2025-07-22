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
    name = 'partial_fork_join', destination = './output',
    keep_traceback=True, rich_types=True
  )
  client.register_plugin(plugin) 
  plugin.start(client.scheduler)

  x = client.submit(inc, 1)
  y = client.submit(inc, 2)
  z = client.submit(add, x, y)
  w = client.submit(square, y)
  
  print(f'z = {z.result()}, w = {w.result()}')

  client.close()
