from typing import Any, cast
from ipykernel.kernelapp import IPKernelApp
import jupyter_client
from jupyter_client.client import KernelClient
from zmq import Socket

TYPE = 'msg_type'
REQUIRED_TYPE = 'execute_input'
EXECUTION_COUNT = 'execution_count'
DELIMITER = '<IDS|MSG>'

def listen(connection):
  client: KernelClient | None = None
  socket: Socket[Any] | None = None

  try:
    app = IPKernelApp.instance()
    config_file = jupyter_client.find_connection_file(app.abs_connection_file)
    client = jupyter_client.blocking.client.BlockingKernelClient(connection_file=config_file)
    client.load_connection_file()
    socket = client.connect_iopub()
    connection.send(True)
  except Exception as e:
    connection.send(False)
    connection.close()
    # Terminate the thread
    return

  if socket is not None:
    # Terminate when the plugin sends a message
    while not connection.poll():
      obj = ''
      while obj != DELIMITER:
        try:
          obj = socket.recv_string()
        except Exception as e:
          break
      
      socket.recv()   # hmac_signature
      header = cast(dict[str, Any], socket.recv_json())
      socket.recv()   # parent_header
      socket.recv()   # metadata
      content = cast(dict[str, Any], socket.recv_json())

      if header[TYPE] == REQUIRED_TYPE:
        cell_id: int = content[EXECUTION_COUNT]
        connection.send(cell_id)

  connection.close()
      