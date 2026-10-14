-- Curated / Gold: dimensão de clientes com métricas de ciclo de vida. Grão: 1 linha por cliente.
--
-- Decisões de modelagem física:
--   * Particionada por mês de cadastro (registered_month): análises de coorte filtram por
--     safra de cadastro e se beneficiam do pruning. Em uma dimensão pequena isso é opcional;
--     a escolha está documentada no README.
--   * Clusterizada por país e segmento, que são os filtros mais usados nos dashboards.
--   * No sandbox do BigQuery (ver macro is_sandbox) fica sem particionamento: a expiração
--     de 60 dias por partição apagaria todo cliente cadastrado há mais de 2 meses.
{{
    config(
        materialized='table',
        partition_by=(none if is_sandbox() else {
            'field': 'registered_month',
            'data_type': 'date',
            'granularity': 'month'
        }),
        cluster_by=['country', 'customer_segment']
    )
}}

with users as (

    select * from {{ ref('stg_thelook__users') }}

),

orders as (

    select * from {{ ref('int_orders__aggregated') }}

),

country_regions as (

    select * from {{ ref('country_regions') }}

),

customer_orders as (

    select
        user_id,
        min(order_date)                                    as first_order_date,
        max(order_date)                                    as last_order_date,
        count(*)                                           as total_orders,
        countif(order_status = 'cancelled')                as cancelled_orders,
        sum(item_count)                                    as total_items,
        sum(gross_revenue)                                 as lifetime_gross_revenue,
        sum(net_revenue)                                   as lifetime_net_revenue,
        sum(gross_margin)                                  as lifetime_gross_margin
    from orders
    group by user_id

),

final as (

    select
        users.user_id,
        concat(users.first_name, ' ', users.last_name)     as full_name,
        users.email,
        users.age,
        users.gender,
        users.city,
        users.state,
        users.country,
        coalesce(country_regions.region, 'Other')          as region,
        users.traffic_source,
        users.registered_at,
        date_trunc(date(users.registered_at), month)       as registered_month,

        customer_orders.first_order_date,
        customer_orders.last_order_date,
        coalesce(customer_orders.total_orders, 0)          as total_orders,
        coalesce(customer_orders.cancelled_orders, 0)      as cancelled_orders,
        coalesce(customer_orders.total_items, 0)           as total_items,
        coalesce(customer_orders.lifetime_gross_revenue, 0) as lifetime_gross_revenue,
        coalesce(customer_orders.lifetime_net_revenue, 0)  as lifetime_net_revenue,
        coalesce(customer_orders.lifetime_gross_margin, 0) as lifetime_gross_margin,
        safe_divide(customer_orders.lifetime_net_revenue, customer_orders.total_orders) as avg_order_value,

        -- Segmentação simples de ciclo de vida (regra de negócio ilustrativa)
        case
            when customer_orders.total_orders is null                 then 'prospect'
            when customer_orders.lifetime_net_revenue >= 500          then 'vip'
            when customer_orders.total_orders = 1                     then 'new'
            else 'returning'
        end                                                as customer_segment

    from users
    left join customer_orders
        on users.user_id = customer_orders.user_id
    left join country_regions
        on users.country = country_regions.country

)

select * from final
