from dask.task_spec import DataNode, Task, Alias
from dask.typing import Key
from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.scheduler import Scheduler, TaskState, TaskStateState as SchedulerTaskState
from prov_tracking.documenter import Documenter
from prov_tracking.utils import RunnableTaskInfo

import datetime as dt
from typing import Any

class ProvTracker(SchedulerPlugin):
  """Provenance tracking plugin"""

  def __init__(self, **kwargs):
    # kwargs that are only used by the plugin, should be removed because some
    # methods used by libraries, unpack kwargs and any additional argument would
    # cause an exception
    
    self.keep_stacktrace: bool = kwargs.pop('keep_stacktrace', False)
    self.documenter = Documenter(**kwargs)
    self.destination: str = kwargs.get('destination')
    self.kwargs: dict[str, Any] = kwargs
    self.closed = False
    # Used to avoid registering multiple times the same task. A task can be put
    # multiple times in waiting state
    self.tasks: set[Key] = set()
    self.runnables: dict[key, RunnableTaskInfo] = {}

  def start(self, scheduler: Scheduler):
    self._scheduler = scheduler

  def transition(
    self, key: Key, start: SchedulerTaskState, finish: SchedulerTaskState,
    *args, **kwargs
  ):
    try:
      task = self._scheduler.tasks[key]

      #if start == 'released' and finish == 'waiting':
        #print(f'Relased: {key}:\n\tSPEC:{task.run_spec}\n\tDEPENDENTS:{task.dependents}\n\tDEPENCIES:{task.dependencies}')
      # A new task has been created: register the activity and its data
      if start == 'waiting' and key not in self.tasks:
        # This is a never-seen-before task. Register it as an entity if it is a
        # non-runnable task. Otherwise, register its info are registered in an 
        # internal structure that will be saved as an activity later on.

        self.tasks.add(key)
        if isinstance(task.run_spec, DataNode):
          try:
            self.documenter.register_non_runnable_task(str(key), task.run_spec)
          except Exception as e:
            print(f'Waiting {key}: {e}')
        elif isinstance(task.run_spec, Task):
          if ProvTracker._is_dask_internal(task.run_spec):
            new_task = task.run_spec.args
            pass
          self.runnables[key] = RunnableTaskInfo(task)
        else: # Task.run_spec is of type Alias
          target = self._scheduler.tasks[task.run_spec.target]
          #print(f'{key} [{type(task.run_spec)}]: {task.run_spec}')
      elif start == 'processing' and key in self.runnables:
        info: RunnableTaskInfo = self.runnables[key]
        info.start_time = dt.datetime.now()
      elif start == 'memory' and key in self.runnables:
        info: RunnableTaskInfo = self.runnables[key]
        info.finish_time = dt.datetime.now()
        dtype = task.type
        nbytes = task.nbytes
        try:
          self.documenter.register_successful_task(info, dtype, nbytes)
        except Exception as e:
          print(f'Memory {key}: {e}')
      elif start == 'memory' and isinstance(task.run_spec, Alias):
        target = self._scheduler.tasks[task.run_spec.target]
        self.documenter.register_alias(task, target)
      elif start == 'erred' and key in self.runnables:
        info: RunnableTaskInfo = self.runnables[key]
        info.finish_time = dt.datetime.now()
        # A task is finished with an error, so register the exception
        text = task.exception_text
        blamed_task = task.exception_blame
        stacktrace = None
        if self.keep_stacktrace:
          stacktrace = task.traceback_text
        
        try:
          self.documenter.register_failed_task(info, text, stacktrace, blamed_task)
        except Exception as e:
          print(f'Erred {key}: {e}')

        # When an exception occurs, the plugin is closed before it has the chance
        # to detect the erred task and register its information. So, if the plugin
        # has already been closed, serialize the document again
        if self.closed:
          self.serialize_document()
    except Exception as e:
      print(f'Task {key} generated exception at line {e.__traceback__.tb_lineno}:\n{e}')

  async def close(self):
    self.closed = True
    ser_str = self.serialize_document()
    if ser_str is not None:
      print(ser_str)

  @staticmethod
  def _is_dask_internal(task: Task) -> bool:
    func = task.func
    if hasattr(func, '__name__'):
      return (
        func.__name__ == '_execute_subgraph' and
        func.__module__ == 'dask._task_spec'
      )
    return false

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

  def serialize_document(self, **kwargs: dict[str, Any]) -> str | None:
    return self.documenter.serialize()
