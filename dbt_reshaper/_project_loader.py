from .dynamic_modules import DynamicModules
import dbt
from dbt.config.project import _load_yaml
import os
from .reshaper_logs import fire_info_event

current_directory = os.path.dirname(__file__)

_reshaper_configured_cache = []
def _load_yaml_with_reshaper(path):
    yaml_config = _load_yaml(path)
    if path.endswith('dbt_project.yml'):
        if 'reshaper' in yaml_config:
            reshaper_config = yaml_config['reshaper']
            root_dir = os.path.dirname(path)
            if path not in _reshaper_configured_cache:
                _apply_reshaper_configuration(yaml_config, reshaper_config, root_dir)
                _reshaper_configured_cache.append(path)
            yaml_config.pop('reshaper')
    return yaml_config

def _apply_reshaper_configuration(yaml_config, reshaper_config, root_dir):

    ### START define functions to apply ###
    def load_aliases(modules_config):
        if 'alias' in modules_config:
            alias = modules_config['alias']
            for module_name in alias:
                if alias not in DynamicModules.dynamic_modules_alias:
                    DynamicModules.dynamic_modules_alias.append(module_name)

    def preload_dynamic_modules(modules_config):
        if 'preload' in modules_config:
            modules = modules_config['preload']
            for module_name in modules:
                if module_name not in DynamicModules.dynamic_modules:
                    DynamicModules.load_module(module_name, modules[module_name])
                    fire_info_event("DRI-XX", "<Dynamic module loaded> " + module_name)

    def extend_docs_support(reshaper_config, root_dir):
        if 'index-file' in reshaper_config['docs']:
            index_file_path = os.path.join(current_directory, reshaper_config['docs']['index-file'])
            index_file_normpath = os.path.normpath(index_file_path)
            dbt.include.global_project.DOCS_INDEX_FILE_PATH = index_file_normpath
        if 'preload-module' in reshaper_config['docs']:
            DynamicModules.load_module('reshaper_docs', current_directory + '/modules/docs/__init__.py')
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
                os.environ['DBT_MACROS_PATH'] = DynamicModules.macro_paths[0] + '/'
                fire_info_event("DRI-XX", "<Setting env DBT_MACROS_PATH to> " + os.environ['DBT_MACROS_PATH'])
    ### DONE ###
    if 'dynamic_modules' in reshaper_config:
        fire_info_event("DRI-XX", "<Found config in dbt_project.yml> reshaper.modules")
        load_aliases(reshaper_config['dynamic_modules'])
        preload_dynamic_modules(reshaper_config['dynamic_modules'])
    if 'headers' in reshaper_config:
        fire_info_event("DRI-XX", "<Found config in dbt_project.yml> reshaper.headers")
        DynamicModules.headers = reshaper_config['headers']
    if 'docs' in reshaper_config:
        fire_info_event("DRI-XX", "<Found config in dbt_project.yml> reshaper.docs")
        extend_docs_support(reshaper_config, root_dir)
    resolve_macros_path(yaml_config, root_dir)


dbt.config.project._load_yaml = _load_yaml_with_reshaper