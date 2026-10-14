{#-
  Restringe o volume lido em ambiente de desenvolvimento.
  Em target dev, acrescenta um filtro temporal a clausula WHERE do modelo;
  em qualquer outro target (ex.: prod) nao adiciona nada.

  Uso (dentro de um WHERE ja existente):
      where id is not null
      {{ limit_data_in_dev('created_at') }}
-#}
{% macro limit_data_in_dev(column_name, days=var('dev_lookback_days')) %}
    {%- if target.name == 'dev' -%}
        and {{ column_name }} >= timestamp_sub(current_timestamp(), interval {{ days }} day)
    {%- endif -%}
{% endmacro %}
