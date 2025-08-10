from typing import Any
from dask.task_spec import DataNode
from distributed.scheduler import TaskState

from prov_tracking.utils import GeneratedValue, ReadyValue, Value
from prov_tracking.task_info import RunnableTaskInfo
from yprov4wfs.datamodel.workflow import Workflow
from yprov4wfs.datamodel.data import Data
from yprov4wfs.datamodel.task import Task
from uuid import uuid4

def _sanitize(string: str) -> str:
  """Given a string, returns a new string without `(`, `)`, `\\` and with
  `,` substituted by `_`."""

  return string.replace('(', '').replace(')', '').replace('\'', '').replace(', ', '_')

def _serialize_value(obj: Any) -> str:
  """Ginen an object, serializes it as a string."""

  if isinstance(obj, str):
    return obj
  elif isinstance(obj, (list, tuple)):
    tmp_list = [ _serialize_value(x) for x in obj ]
    if isinstance(obj, tuple):
      return str(tuple(tmp_list))
    else:
      return str(tmp_list)
  elif isinstance(obj, dict):
    return str({ k: _serialize_value(v) for k, v in obj.items() })
  else:
    return str(obj)

def _type(obj: Any) -> str:
  """Given and object, returns a string representing its type. The string is
  richer than that produced by simply calling `str(type(obj))`."""

  if isinstance(obj, list):
    if len(obj) == 0:
      return 'list[()]'
    else:
      return f'list[{_type(obj[0])}]'
  if isinstance(obj, tuple):
    if len(obj) == 0:
      return 'tuple[()]'
    else:
      types = []
      for item in obj:
        types.append(_type(item))
      return f'tuple[{', '.join(types)}]'
  if not isinstance(obj, type):
    obj = type(obj)
  module = obj.__module__
  if module != 'builtins':
    return f'{module}.{obj.__qualname__}'
  else:
    return obj.__qualname__
    

class Documenter:
  """Handles the creation of entities and activities associated with tasks."""

  def __init__(self, name: str, **kwargs):
    """Additional arguments:
    - `destination: str`: folder in which the provenance graph is saved. The file is
    always named `yprov4wfs.json`. Defaults to `./output`.
    - `rich_types: bool`: tells if datatypes of values such be richer, e.g. for
    tuples, track the type of each element instead of just saying that the value
    is a tuple. Defaults to `False`.
    """
    
    self.destination: str = kwargs.pop('destination', './output')
    self.rich_types: bool = kwargs.pop('rich_types', False)

    self.workflow = Workflow(id = str(uuid4()), name=name)
    self.data = {}
    self.tasks = {}

  def register_data(self, datanode: DataNode):
    """Non-runnable tasks are registered as entities as they are in fact just data"""
    
    data_id = _sanitize(str(datanode.key))
    data = Data(id=data_id, name=data_id)
    data.type = str(type(datanode.typ)) if not self.rich_types else _type(datanode.typ)
    data._info = {
      'value': _serialize_value(datanode.value),
      'dtype': data.type
    }

    self.workflow.add_data(data)
    self.data[data_id] = data

  def _register_task_param(self, task_id: str, name: str, param: Value) -> tuple[str, str]:
    """Registers the param name for activity `activity_id` according to its
    value. If it is a `ReadyValue`, an entity is created for the parameter and
    the pair `(name, key)` is returned, with `key` being the identifier of the
    created entity. If instead param is a `GeneratedValue`, the returned pair
    has the identifier of the entity that represents the return value of the
    generator activity.
    """
    param_id = None
    if isinstance(param, ReadyValue):
      param_id = _sanitize(param.key)
    elif isinstance(param, GeneratedValue):
      param_id = f'{_sanitize(param.generatedBy)}.return_value'
    else:
      param_id = f'{task_id}.{name}'
      data = Data(id=param_id, name=param_id)
      data.type = str(type(param.value)) if not self.rich_types else _type(param.value)
      data._info = {
        'value': _serialize_value(param.value),
        'dtype': data.type
      }
      self.workflow.add_data(data)
      self.data[param_id] = data
      
    return (name, param_id)

  def register_task_dependencies(self, info: RunnableTaskInfo):
    """Parameters to the task are registered as entities linked to the activity
    via used relations. All runnable dependencies of the task are registered via
    communication relations."""
    task_id = _sanitize(str(info.key))
    task = self.tasks[task_id]

    used_params = []
    for name, param in info.args_dict.items():
      if not isinstance(param, set):
        used_params.append(self._register_task_param(task._id, name, param))
      else:
        for value in param:
          used_params.append(self._register_task_param(task._id, name, value))
    for name, data_id in used_params:
      try:
        data: Data = self.data[data_id]
        data.add_consumer(task)
        task.add_input(data)
      except Exception as e:
        print(f'Warning: missing data_id for {info.key}(.., {name}=..): {e}')
    try:
      for informant_key in info.informants:
        informant_id = _sanitize(str(informant_key))
        informant_task: Task = self.tasks[informant_id]
        task.add_prev(informant_task)
        informant_task.add_next(task)
    except Exception as e:
      print(f'Warning: missing informant_id for {info.key}: {e}')

  def register_task_worker(self, info: RunnableTaskInfo):
    """Registers the worker on which the task was processed."""

    task_id = _sanitize(str(info.key))
    task = self.tasks[task_id]
    task._info['processed_on'] = info.processed_on
  
  def register_task(self, info: RunnableTaskInfo) -> Task:
    """Runnble tasks are registered as activities and their returned values as
    entities. No other info is recorded here. For registering dependencies see
    `Documenter.register_task_dependencies`"""

    task_id = _sanitize(str(info.key))
    attributes = {
      'processed_on': info.processed_on,
      'group': info.group,
      'module': info.func.__module__,
      'name': str(info.func),
    }
    if hasattr(info.func, '__name__'):
      attributes['nice_name'] = info.func.__name__
    if info.jupyter_cell is not None:
      attributes['jupyter_cell'] = info.jupyter_cell
    task = Task(id=task_id, name=task_id)
    task._info = attributes
    if task_id not in self.tasks:
      self.workflow.add_task(task)
      self.tasks[task_id] = task
      result_id = f'{task._id}.return_value'
      result = Data(id=result_id, name=result_id)
      result.set_producer(task)
      task.add_output(result)
      self.workflow.add_data(result)
      self.data[result_id] = result
    
    return task

  def register_task_success(
    self, info: RunnableTaskInfo, dtype: str | None, nbytes: int | None
  ):
    """Registers the successful completion of a runnble tasks and the value that
    the task produced."""

    task_id = _sanitize(str(info.key))
    task: Task = self.tasks[task_id]
    task._status = 'success'
    task._start_time = info.start_time
    task._end_time = info.finish_time

    # Records the value generated by this funcion
    result = task._outputs[0] # Tasks always have exactly one output
    attributes = {}
    if dtype is not None and nbytes is not None:
      attributes = {
        'dtype': dtype,
        'nbytes': str(nbytes)
    }
    result._info = attributes

  def register_task_failure(
    self, info: RunnableTaskInfo, exception_text: str | None,
    traceback: str | None, blamed_task: TaskState | None
  ):
    """Registers the failed completion of some runnble tasks and records the
    exception they raised."""

    task_id = _sanitize(str(info.key))
    task: Task = self.tasks[task_id]
    task._status = 'failure'
    task._start_time = info.start_time
    task._end_time = info.finish_time

    # Records the value generated by this funcion
    result = task._outputs[0] # Tasks always have exactly one output
    attributes = {}  
    attributes: dict[str, Any] = {
      'is_error': True,
    }
    if exception_text is not None:
      attributes['exception_text'] = exception_text
    
      if traceback is not None:
        attributes['traceback'] = traceback
      if blamed_task is not None and blamed_task.key != task._id:
        other_task_id = _sanitize(str(blamed_task.key))
        attributes['blamed_task'] = other_task_id
    result._info = attributes

  def serialize(self, destination=None):
    """Serializes the provenance document into `destination`."""

    if destination is None and self.destination is not None:
      destination = self.destination
    self.workflow.prov_to_json(directory_path=destination)
