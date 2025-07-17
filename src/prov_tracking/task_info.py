import inspect
from datetime import datetime
from dask.task_spec import Alias, DataNode, List, Task
from dask.typing import Key

from prov_tracking.utils import GeneratedValue, Value, get_value, get_values_from_list

class RunnableTaskInfo:
  """Container class holding info about a runnable task."""

  def __init__(self, key: Key, group_key: Key, specs: Task):
    self.key = key
    self.group = group_key
    self.func = specs.func
    self._specs = specs
    self.start_time: datetime | None = None
    self.finish_time: datetime | None = None
    self.args_dict: dict[str, Value | set[Value]] = {}
    self.informants: list[Key] = []

  def record_dependencies(
    self,
    dependencies: dict[Key, Task | DataNode | Alias],
    all_tasks: dict[Key, Task | DataNode],
    unique_keys: dict[Key, Key],
    pending_tasks: list[tuple[Key, Task]]
  ):
    """Updates the object recording its dependencies, i.e. what values are used
    for each argument or what task must be looked at to retrive them and also
    what tasks are informant to this one."""

    param_names = []
    try:
      param_names = list(inspect.signature(self.func).parameters)
    except ValueError:
      # The signature in non-inspectable
      param_names = [f'arg_{i}' for i in range(len(self._specs.args))]

    for name, value in zip(param_names, self._specs.args):
      if isinstance(value, List):
        # Multiple tasks cooperate to produce this value. Maybe it's a list of
        # values returned by some tasks
        values = set()
        get_values_from_list(value, values, all_tasks, dependencies, unique_keys, pending_tasks, self.key)
        self.args_dict[name] = values
      else:
        self.args_dict[name] = get_value(value, all_tasks, dependencies, unique_keys, pending_tasks, self.key)

    if len(self._specs.args) > len(param_names):
      values = set()
      values.add(self.args_dict[param_names[-1]])
      for value in self._specs.args[len(param_names):]:
        if isinstance(value, List):
          get_values_from_list(value, values, all_tasks, dependencies, unique_keys, pending_tasks, self.key)
        else:
          values.add(get_value(value, all_tasks, dependencies, unique_keys, pending_tasks, self.key))
      self.args_dict[param_names[-1]] = values

    for name, value in self._specs.kwargs.items():
      if isinstance(value, List):
        values = set()
        get_values_from_list(value, values, all_tasks, dependencies, unique_keys, pending_tasks, self.key)
        self.args_dict[name] = values
      else:
        self.args_dict[name] = get_value(value, all_tasks, dependencies, unique_keys, pending_tasks, self.key)

    if str(self.key).startswith('finalize'):
      pass
    for v in self.args_dict.values():
      if isinstance(v, set):
        for item in v:
          if isinstance(item, GeneratedValue):
            self.informants.append(item.generatedBy)
      else:
        if isinstance(v, GeneratedValue):
          self.informants.append(v.generatedBy)