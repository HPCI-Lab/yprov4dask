from datetime import datetime
from uuid import uuid4
from dask.task_spec import Task, DataNode, Alias, TaskRef, List
from dask.typing import Key
from distributed.scheduler import TaskState

from collections.abc import Callable
from typing import Any, cast
import inspect

class RunnableTaskInfo:
  """Container class holding info about a runnable task."""

  def __init__(
    self,
    task: TaskState | None = None,
    key: Key | None = None,
    specs: Task | None = None,
    group_key: str | None = None,
    dependencies: dict[Key, Task | DataNode | Alias] = {},
    all_tasks: dict[Key, Task | DataNode] = {},
    unique_keys: dict[Key, Key] = {},
    pending_tasks: list[tuple[Key, Task]] = []
  ):
    if task is not None:
      self._from_task(task, all_tasks)
    elif key is not None and specs is not None and group_key is not None:
      self._from_pieces(specs, group_key, dependencies, unique_keys, all_tasks)
    else:
      raise ValueError('''
RunnableTaskInfo can take either a TaskState object or key, specs and group_key.
Dependencies are also likely needed it the task has any. Optionally, it accepts
a dictionary of internal dependencies through `all_tasks`, i.e. dependencies
which are referenced with a locally-defined key, and a dictionary mapping
non-unique keys to their unique alternative. As it might happen that some task
references other tasks which never pass through the scheduler, `pending_tasks`
is a list populated with these tasks that will have to be dealt with after this
method completes.
      ''')
    self.start_time: datetime | None = None
    self.finish_time: datetime | None = None

    specs = cast(Task, specs or task.run_spec)
    self.args_dict: dict[str, Value | set[Value]] = {}
    
    param_names = []
    try:
      param_names = list(inspect.signature(self.func).parameters)
    except ValueError:
      # The signature in non-inspectable
      param_names = [f'arg_{i}' for i in range(len(specs.args))]

    for name, value in zip(param_names, specs.args):
      if isinstance(value, List):
        # Multiple tasks cooperate to produce this value. Maybe it's a list of
        # values returned by some tasks
        values = set()
        _get_values_from_list(value, values, all_tasks, dependencies, unique_keys, pending_tasks)
        self.args_dict[name] = values
      else:
        self.args_dict[name] = _get_value(value, all_tasks, dependencies, unique_keys, pending_tasks)

    if len(specs.args) > len(param_names):
      values = set()
      values.add(self.args_dict[param_names[-1]])
      for value in specs.args[len(param_names):]:
        if isinstance(value, List):
          _get_values_from_list(value, values, all_tasks, dependencies, unique_keys, pending_tasks)
        else:
          values.add(_get_value(value, all_tasks, dependencies, unique_keys, pending_tasks))
      self.args_dict[param_names[-1]] = values

    for name, value in specs.kwargs.items():
      if isinstance(value, List):
        values = set()
        _get_values_from_list(value, values, all_tasks, dependencies, unique_keys, pending_tasks)
        self.args_dict[name] = values
      else:
        self.args_dict[name] = _get_value(value, all_tasks, dependencies, unique_keys, pending_tasks)

  def _from_task(self, task: TaskState, all_tasks: dict[Key, Task | DataNode]):
    self.key: Key = task.key
    specs: Task = cast(Task, task.run_spec)
    self.func: Callable = specs.func
    self.group: str = task.group_key
    self.informants: list[Key] = []
    for dep in task.dependencies:
      if isinstance(dep.run_spec, Task):
        self.informants.append(dep.key)
      elif isinstance(dep.run_spec, Alias):
        target = all_tasks[dep.run_spec.target]
        if isinstance(target, Task):
          self.informants.append(target.key)

  def _from_pieces(
    self, specs: Task, group_key: str,
    dependencies: dict[Key, Task | DataNode | Alias],
    unique_keys: dict[Key, Key],
    all_tasks: dict[Key, Task | DataNode]
  ):
    self.group: str = group_key
    self.key: Key = unique_keys.get(specs.key, specs.key)
    self.func: Callable = specs.func
    self.informants: list[Key] = []
    for dep in dependencies.values():
      if isinstance(dep, Task):
        self.informants.append(unique_keys.get(dep.key, dep.key))
      elif isinstance(dep, Alias):
        target = unique_keys.get(dep.target, dep.target)
        task = all_tasks[target]
        if isinstance(task, Task):
          self.informants.append(unique_keys.get(task.key, task.key))

class RawValue:
  """A value already available and is not associated to a `DataNode`, e.g. an
  integer value o a string."""
  def __init__(self, value: Any):
    self.value = value

  def __eq__(self, o: object) -> bool:
    if isinstance(o, RawValue):
      return self.value == o.value
    return False
  
  def __hash__(self) -> int:
    return hash(('value', repr(self.value)))

class ReadyValue:
  """A value already available, but that is associated to a `DataNode`, i.e. the
  `value` attribute of a `DataNode` object."""

  def __init__(self, key: str, value: Any):
    self.key = key
    self.value = value

  def __eq__(self, o: object) -> bool:
    if isinstance(o, ReadyValue):
      return self.value == o.value and self.key == o.key
    return False
  
  def __hash__(self) -> int:
    return hash(('key', self.key, 'value', repr(self.value)))

class GeneratedValue:
  """A value which has been generated by another task."""

  def __init__(self, generator: str):
    self.generatedBy = generator

  def __eq__(self, o: object) -> bool:
    if isinstance(o, GeneratedValue):
      return self.generatedBy == o.generatedBy
    return False

  def __hash__(self) -> int:
    return hash(('generatedBy', self.generatedBy))

type Value = GeneratedValue | ReadyValue | RawValue

def _get_value(
  obj: Any, all_tasks: dict[Key, Task | DataNode],
  dependencies: dict[Key, Task | DataNode | Alias | Any],
  unique_keys: dict[Key, Key],
  pending_tasks: list[tuple[Key, Task]]
) -> Value:
  """Given a parameter value creates a suitable representation for it. If the
  value comes from another task, returns a `GeneratedValue`, otherwise returns
  a `ReadyValue`."""

  if isinstance(obj, TaskRef):
    task = None
    if obj.key in dependencies:
      task = dependencies[obj.key]
    else:
      key = unique_keys.get(obj.key, obj.key)
      task = all_tasks[key]
      # Sometimes key is unique and task.key isn't, but they refer to the same task
      if task.key not in unique_keys and are_equal_keys(task.key, key):
        unique_keys[task.key] = key
    return _get_value(task, all_tasks, dependencies, unique_keys, pending_tasks)
  elif isinstance(obj, Alias):
    target = unique_keys.get(obj.target, obj.target)
    task = None
    if target in dependencies:
      task = dependencies[target]
    else:
      task = all_tasks[target]
    return _get_value(task, all_tasks, dependencies, unique_keys, pending_tasks)
  elif isinstance(obj, Task):
    key = unique_keys.get(obj.key, obj.key)
    if obj.key in dependencies or key in all_tasks:
      return GeneratedValue(str(key))
    else:
      # Create a new key and later register the new task
      func_name = obj.func.__name__
      new_key = f'{func_name}-{uuid4()}'
      new_task = Task(new_key, obj.func, *obj.args, **obj.kwargs)
      pending_tasks.append((new_key, new_task))
      return GeneratedValue(str(new_key))
  elif isinstance(obj, DataNode):
    if obj.key in all_tasks:
      return ReadyValue(str(obj.key), obj.value)
    else:
      return RawValue(obj.value)
  else:
    return RawValue(obj)

def _get_values_from_list(
  obj: Any, items: set, all_tasks: dict[Key, Task | DataNode],
  dependencies: dict[Key, Task | DataNode | Alias | Any],
  unique_keys: dict[Key, Key],
  pending_tasks: list[tuple[Key, Task]]
):
  """Recursively takes all items from a list and its sublists"""

  if not isinstance(obj, List):
    items.add(_get_value(obj, all_tasks, dependencies, unique_keys, pending_tasks))
  else:
    for item in obj:
      _get_values_from_list(item, items, all_tasks, dependencies, unique_keys, pending_tasks)

def make_unique_key(parent: Key, child: Key) -> Key:
  """Takes the key of a task `child` which has been started by `parent`, i.e.
  child is part of the `inner_dsk` dictionary of `parent.args`. If `child` is
  a non-unique key, returns a new key that embeds `parent`'s info to produce
  a new unique key."""

  key = child
  # This first condition should always be True
  if isinstance(parent, tuple) and len(parent) > 1:
    if isinstance(child, tuple) and len(child) == 1:
      key = (child[0], *parent[1:])
    elif not isinstance(child, tuple):
      key = (child, *parent[1:])
    # Otherwise, the child key is fine as it is
  return key

def are_equal_keys(key1: Key, key2: Key) -> bool:
  str1 = key1
  if isinstance(key1, tuple):
    str1 = key1[0]
  str2 = key2
  if isinstance(key2, tuple):
    str2 = key2[0]
  return str1 == str2
  