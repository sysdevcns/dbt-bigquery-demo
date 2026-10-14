-- Curated / Gold: receita diária por departamento e categoria, pronta para o BI/financeiro.
-- Grão: 1 linha por (order_date, product_department, product_category).
--
-- Particionada por order_date e clusterizada por departamento/categoria: um dashboard que
-- filtra "últimos 90 dias" e agrupa por categoria lê só 90 partições e blocos já ordenados.
{{
    config(
        materialized='table',
        partition_by={
            'field': 'order_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['product_department', 'product_category']
    )
}}

with order_items as (

    select * from {{ ref('fct_order_items') }}

),

daily as (

    select
        order_date,
        coalesce(product_department, 'Unknown')      as product_department,
        coalesce(product_category, 'Unknown')        as product_category,

        count(distinct order_id)                     as orders,
        count(distinct user_id)                      as customers,
        count(*)                                     as items,
        countif(is_valid_sale)                       as items_sold,
        countif(is_returned)                         as items_returned,
        countif(is_cancelled)                        as items_cancelled,

        sum(sale_price)                              as gross_revenue,
        sum(net_revenue)                             as net_revenue,
        sum(case when is_returned then sale_price else 0 end)  as returned_revenue,
        sum(case when is_cancelled then sale_price else 0 end) as cancelled_revenue,
        sum(case when is_valid_sale then gross_margin else 0 end) as gross_margin,
        safe_divide(
            sum(case when is_valid_sale then gross_margin else 0 end),
            sum(net_revenue)
        )                                            as gross_margin_pct,
        safe_divide(sum(net_revenue), count(distinct order_id)) as avg_order_value

    from order_items
    group by order_date, product_department, product_category

)

select
    {{ dbt_utils.generate_surrogate_key(['order_date', 'product_department', 'product_category']) }} as daily_revenue_key,
    *
from daily
