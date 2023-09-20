{#```doc
description: |
    Takes a duckdb query and create a temporary athena/glue table which gets dropped at the end of the pipeline run
arguments:
    - name: duck_query
      type: string
      description: A duckdb compatible query
```#}
{% macro glueduck_query_to_glue_table(duck_query) %}
    {% if execute %}
        {{ modules.dynamic.inc['dbt_glueduck'].query_to_glue_table(duck_query) }}
    {% endif %}
{% endmacro %}