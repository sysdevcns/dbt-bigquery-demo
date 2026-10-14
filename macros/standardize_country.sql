{#-
  Padroniza nomes de pais que chegam inconsistentes na fonte
  (thelook_ecommerce mistura "Brasil"/"Brazil", "Deutschland"/"Germany", "España"/"Spain").
  Centralizar a regra em um macro evita repetir o CASE em varios modelos.
-#}
{% macro standardize_country(column_name) %}
    case trim({{ column_name }})
        when 'Brasil'      then 'Brazil'
        when 'Deutschland' then 'Germany'
        when 'España'      then 'Spain'
        when 'Espanha'     then 'Spain'
        else nullif(trim({{ column_name }}), '')
    end
{% endmacro %}
