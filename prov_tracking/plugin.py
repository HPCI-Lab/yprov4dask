from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.scheduler import Scheduler, TaskStateState as SchedulerTaskState
from dask.typing import Key
from prov_tracking.documenter import Documenter
from prov.serializers.provjson import encode_json_document
import inspect
import io

class ProvTracker(SchedulerPlugin):
  """Prov tracking plugin"""

  def __init__(self, **kwargs):
    self.documenter = Documenter(**kwargs)
    self.pendingActivities: dict[Key, list[str]] = {}
    self.kwargs = kwargs

  def start(self, scheduler: Scheduler):
    self._scheduler = scheduler

  def transition(
    self, key: Key, start: SchedulerTaskState, finish: SchedulerTaskState,
    *args, **kwargs
  ):
    task = self._scheduler.tasks[key]
    # A new task has been assigned to a worker for compute: register the activity
    # and other related data
    if start == 'processing':
      print(f'new task: {key}')
      # Registers arguments and activity
      func = task.run_spec[0]
      args = task.run_spec[1]
      kwargs = task.run_spec[2]
      args_dict = {}
      param_names = list(inspect.signature(func).parameters)
      for name, value in zip(param_names, args):
        args_dict[name] = value
      args_dict.update(kwargs)
      try:
        act, args = self.documenter.register_function_call(
          key, func.__name__, func.__module__,
          args_dict, task.group_key, task.dependencies
        )
      except Exception as e:
        print(f'Task {key}: {e}')
      self.pendingActivities.setdefault(act, args)
    elif start == 'memory':
      t = task.type
      size = task.nbytes
      self.documenter.register_function_result(key, t, size)
      self.pendingActivities.pop(key)
    elif start == 'erred':
      pass
    else:
      pass
      

  async def close(self):
    ser_str = self.documenter.serialize(**self.kwargs)
    if 'destination' not in self.kwargs:
      print(ser_str)