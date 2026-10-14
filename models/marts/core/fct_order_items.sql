-- Curated / Gold: fato de itens de pedido. Grão: 1 linha por item.
--
-- Decisões de modelagem física:
--   * Incremental com insert_overwrite: a cada execução só as partições dos últimos
--     `incremental_lookback_days` dias são recalculadas e substituídas (idempotente).
--   * copy_partitions=true usa a API de cópia do BigQuery em vez de MERGE/DELETE,
--     o que funciona no sandbox (que não permite DML) e é mais barato.
--   * Particionado por order_date (dia): as consultas financeiras quase sempre filtram
--     por período, então o BigQuery lê só as partições necessárias.
--   * Clusterizado por status e categoria: são os filtros/agrupamentos mais comuns
--     depois da data.
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day',
            'copy_partitions': true
        },
        cluster_by=['order_item_status', 'product_category'],
        on_schema_change='append_new_columns'
    )
}}

with order_items as (

    select * from {{ ref('int_order_items__enriched') }}

    {% if is_incremental() %}
        -- Reprocessa uma janela recente para capturar mudanças de status
        -- (ex.: item enviado -> entregue -> devolvido) sem reconstruir a tabela inteira.
        where order_date >= date_sub(current_date(), interval {{ var('incremental_lookback_days') }} day)
    {% endif %}

)

select
    order_item_id,
    order_id,
    user_id,
    product_id,

    order_date,
    ordered_at,
    shipped_at,
    delivered_at,
    returned_at,
    days_to_ship,
    days_to_deliver,

    product_name,
    product_brand,
    product_category,
    product_department,

    order_item_status,
    is_cancelled,
    is_returned,
    is_valid_sale,

    sale_price,
    product_cost,
    net_revenue,
    gross_margin,
    gross_margin_pct,
    discount_amount

from order_items
