from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.scheduler import Scheduler, TaskState, TaskStateState as SchedulerTaskState
from dask.typing import Key
from prov_tracking.documenter import Documenter
from prov.serializers.provjson import encode_json_document
import inspect
import io

from distributed.worker import execute_task

class ProvTracker(SchedulerPlugin):
  """Prov tracking plugin"""

  def __init__(self, **kwargs):
    # kwargs that are only used by the plugin, should be removed because some
    # methods used by libraries, unpack kwargs and any additional argument would
    # cause an exception
    self.keep_stacktrace: bool = kwargs.pop('keep_stacktrace') or False
    
    self.documenter = Documenter(**kwargs)
    self.destination: str = kwargs.get('destination')
    self.kwargs: dict[str, any] = kwargs
    self.closed = False

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
      func = task.run_spec[0]
      f_args = task.run_spec[1]
      f_kwargs = task.run_spec[2]
      if ProvTracker._is_dask_internal(task):
        func, f_args, f_kwargs = self._track_dask_internal(task)

      args_dict = {}
      param_names = list(inspect.signature(func).parameters)
      for name, value in zip(param_names, f_args):
        args_dict[name] = value
      args_dict.update(f_kwargs)
      try:
        activity, used_params = self.documenter.register_function_call(
          str(key), func.__name__, func.__module__,
          args_dict, task.group_key, task.dependencies
        )
      except Exception as e:
        print(f'Task {key}: {e}')
    elif start == 'memory':
      # A task is finished succesfully, so register its result
      t = task.type
      size = task.nbytes
      self.documenter.register_function_result(str(key), t, size)
    elif start == 'erred':
      # A task is finished with an error, so register the exception
      text = task.exception_text
      blamed_task = task.exception_blame
      traceback = None
      if self.keep_stacktrace:
        traceback = task.traceback_text
      self.documenter.register_function_error(str(key), text, traceback, blamed_task)

      # When an exception occurs, the plugin is closed before it has the chance
      # to detect the erred task and register its information. So, if the plugin
      # has already been closed, serialize the document again
      if self.closed:
        self.serialize_document(**self.kwargs)

  async def close(self):
    self.closed = True
    ser_str = self.serialize_document(**self.kwargs)
    if ser_str is not None:
      print(ser_str)

  @staticmethod
  def _is_dask_internal(task: TaskState) -> bool:
    func = task.run_spec[0]
    return (
      func.__name__ == 'execute_task' and
      func.__module__ == 'distributed.worker'
    )

  def _track_dask_internal(self, task: TaskState):
    # Execute task has always 1 arg, so this is safe
    internal_func = task.run_spec[1][0]

    if isinstance(internal_func, tuple):
      func_id = internal_func[0]

      if isinstance(func_id, str):
        # internal_func is actually the id of another task, so retrieve its info        
        task = self._scheduler.tasks[internal_func]
        func = task.run_spec[0]
        f_args = task.run_spec[1]
        f_kwargs = task.run_spec[2]
        return (func, f_args, f_kwargs)
      elif callable(func_id):
        # Maybe here we can avoid all checks, but some test are required
        if func_id.__module__ == 'functools':
          # This is most probably a functools.partial function
          # Take the actual function being executed and its arguments
          func = func_id.func
          f_args = list(func_id.args)
          f_kwargs = func_id.keywords
          other_args = internal_func[1]
          if (isinstance(other_args, dict)):
            f_kwargs.update(other_args)
          else:
            f_args.extend(list(other_args))
          return (func, f_args, f_kwargs)
        else:
          return (func_id, task.run_spec[1], task.run_spec[2])
    else:
      # func is an object os some kind
      if not callable(internal_func):
        # Since the object is not callable, keep execute_task as function so
        # just return the original specs
        return tuple(task.run_spec)

    return (internal_func, (), {})

  def serialize_document(self, **kwargs: dict[str, any]) -> str | None:
    return self.documenter.serialize(**self.kwargs)
