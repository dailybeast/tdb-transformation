{% macro generate_schema_name(custom_schema_name, node) -%}

    {% if custom_schema_name is none -%}
        {{ default__generate_schema_name(custom_schema_name, node) }}
    
    {%- elif target.name == 'dev' -%}
        dev__{{ custom_schema_name | trim }}
    
    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro -%}