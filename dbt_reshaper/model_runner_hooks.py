from dbt.task.run import ModelRunner
from .reshaper_logs import fire_info_event

run_with_hooks_original = ModelRunner.run_with_hooks

# @TODO implement python hooks for models
def run_with_hooks_override(self: ModelRunner, manifest):
  result = run_with_hooks_original(self, manifest)
  return result

ModelRunner.run_with_hooks = run_with_hooks_override
