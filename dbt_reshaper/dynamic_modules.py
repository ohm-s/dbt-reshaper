from pathlib import Path
import importlib.util
import sys
from dbt.context import base
import os

original_modules = base.get_context_modules

class ImplDynamicModules:

    __all__ = ['load_file_contents', 'load_module']

    macro_paths = []
    dynamic_modules_alias = []
    docs_include_paths = []
    dynamic_modules = {}
    adapter_patches = {}
    headers = {}

    def register_adapter_patch(self, adapter_name, patch):
      if adapter_name not in DynamicModules.adapter_patches:
        DynamicModules.adapter_patches[adapter_name] = []
      if patch not in DynamicModules.adapter_patches[adapter_name]:
        DynamicModules.adapter_patches[adapter_name].append(patch)

    def load_file_contents(self, file_path: str) -> str:
        return Path(file_path).read_text()

    def load_module(self, module_name, file_location):

      spec= importlib.util.spec_from_file_location(module_name, file_location)
      module_spec_loaded = importlib.util.module_from_spec(spec)
      spec.loader.exec_module(module_spec_loaded)
      sys.modules[module_name] = module_spec_loaded
      self.dynamic_modules[module_name] = module_spec_loaded

DynamicModules = ImplDynamicModules()

def get_holidu_module_context():
    context_exports = DynamicModules.__all__
    obj = {name: getattr(DynamicModules, name) for name in context_exports}
    obj['inc'] = DynamicModules.dynamic_modules
    return obj

def get_context_modules_with_dynamic_modules():
    global original_modules
    modules = original_modules()
    dynamic_modules_wrapper = get_holidu_module_context()
    modules['dynamic'] = dynamic_modules_wrapper
    for module in DynamicModules.dynamic_modules_alias:
        modules[module] = dynamic_modules_wrapper
    return modules

base.get_context_modules = get_context_modules_with_dynamic_modules
