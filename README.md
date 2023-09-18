# DBT Reshaper: Monkey Patching for dbt

## Overview

Project Reshaper is an extension to the [dbt](https://www.getdbt.com/) (Data Build Tool) that enables monkey patching of dbt's core functionality.
This project provides a flexible way to override and extend dbt's behavior by allowing you to define custom configurations in your `dbt_project.yml` file. 
With Project Reshaper, you have the power to dynamically load python modules, enhance documentation, and customize headers for your dbt project files, 
effectively monkey patching dbt to meet your unique needs.

## Features

### Dynamic Modules

With Reshaper, you can dynamically load modules using the `dynamic_modules` configuration in your `dbt_project.yml` file. This allows you to monkey patch dbt's core functionality by including additional modules based on your project's requirements. 

Here's an example configuration for dynamic modules:

```yaml
reshaper:
  inject_macros_in_path: true
  dynamic_modules:
    alias:
      - reshaper
    preload:
      your_module: ./macros/__py_dbt_modules__/your_module/__init__.py
      your_other_module: ./macros/__py_dbt_modules__/your_other_module/__init__.py
```

In this example, we've defined a single module alias, 'reshaper,' and specified preload paths for 'your_module' and 'your_other_module.' These modules can be easily loaded and utilized in your dbt project, effectively monkey patching dbt's behavior.

You can view the built-in docs dynamic module on how you can structure your python code

`inject_macros_in_path` will add the dbt_reshaper project macros as part of your project `macros-path`.
This is useful if you don't want to avoid running `dbt deps` 

### Documentation Enhancements

Reshaper also provides tools to enhance your project's documentation. 
You can customize the documentation index file and configure the inclusion of additional paths for documentation files. 

```yaml
reshaper:
  docs:
    index-file: ../index_docs.html
    preload-module: true
    include-paths:
      - markdown/includes
```

### Custom Headers

To further personalize your dbt project, Project Reshaper allows you to define custom headers for your dbt models and tests, etc. 
You can use dynamic values and functions to generate headers automatically. 
Here's how you can configure custom headers in your `dbt_project.yml`:

```yaml
reshaper:
  headers:
    model: "{{- some_config_macro() -}}"
    test: "{{- some_config_macro() -}}"
```

In this example, we're using injecting `some_config_macro()` to the top of every model & test function 

