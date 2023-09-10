from .dynamic_modules import DynamicModules
import dbt
from dbt.config.project import _load_yaml
import os
from .reshaper_logs import fire_info_event

current_directory = os.path.dirname(__file__)

def _load_yaml_dynamic_modules(path):
    yaml_config = _load_yaml(path)
    if path.endswith('dbt_project.yml'):
        if 'reshaper' in yaml_config:
            reshaper_config = yaml_config['reshaper']
            root_dir = os.path.dirname(path)
            _apply_dynamic_modules_changes(yaml_config, reshaper_config, root_dir)
            yaml_config.pop('reshaper')
    return yaml_config

def _apply_dynamic_modules_changes(yaml_config, reshaper_config, root_dir):

    ### START define functions to apply ###
    def load_aliases(modules_config):
        if 'alias' in modules_config:
            alias = modules_config['alias']
            for module_name in alias:
                if alias not in DynamicModules.dynamic_modules_alias:
                    DynamicModules.dynamic_modules_alias.append(module_name)

    def preload_dynamic_mdoules(modules_config):
        if 'preload' in modules_config:
            modules = modules_config['preload']
            for module_name in modules:
                if module_name not in DynamicModules.dynamic_modules:
                    DynamicModules.load_module(module_name, modules[module_name])
                    fire_info_event("DRI-XX", "Dynamic module loaded: " + module_name)

    def extend_docs_support(reshaper_config, root_dir):
        if 'docs' in reshaper_config:
            if 'preload-module' in reshaper_config['docs']:
                DynamicModules.load_module('docs', current_directory + '/modules/docs/__init__.py')
            if 'include-paths' in reshaper_config['docs']:
                include_paths = reshaper_config['docs']['include-paths']
                for include_path in include_paths:
                    # resolve include_path to full path
                    full_dir = root_dir + '/' + include_path
                    if full_dir not in DynamicModules.docs_include_paths:
                        DynamicModules.docs_include_paths.append(full_dir)

    def resolve_macros_path(yaml_config, root_dir):
        if 'macro-paths' in yaml_config:
            macro_paths = yaml_config['macro-paths']
            for macro_path in macro_paths:
                # resolve macro_path to full path
                full_dir = root_dir + '/' + macro_path
                if full_dir not in DynamicModules.macro_paths:
                    DynamicModules.macro_paths.append(full_dir)
                os.environ['DBT_MACROS_PATH'] = DynamicModules.macro_paths[0]
    ### DONE ###
    if 'dynamic_modules' in reshaper_config:
        load_aliases(reshaper_config['dynamic_modules'])
        preload_dynamic_mdoules(reshaper_config['dynamic_modules'])
    extend_docs_support(reshaper_config, root_dir)
    resolve_macros_path(yaml_config, root_dir)


dbt.config.project._load_yaml = _load_yaml_dynamic_modules