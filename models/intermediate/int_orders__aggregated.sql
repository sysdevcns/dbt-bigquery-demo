-- Cleansed / Silver: agrega os itens no grão de pedido. Grão: 1 linha por pedido.
with order_items as (

    select * from {{ ref('int_order_items__enriched') }}

),

aggregated as (

    select
        order_id,
        user_id,

        min(ordered_at)                                   as ordered_at,
        min(order_date)                                   as order_date,
        max(delivered_at)                                 as last_delivered_at,

        count(*)                                          as item_count,
        countif(is_cancelled)                             as cancelled_item_count,
        countif(is_returned)                              as returned_item_count,

        sum(sale_price)                                   as gross_revenue,
        sum(net_revenue)                                  as net_revenue,
        sum(case when is_valid_sale then gross_margin else 0 end) as gross_margin,

        -- Situação consolidada do pedido a partir dos itens
        case
            when countif(is_cancelled) = count(*) then 'cancelled'
            when countif(is_returned) = count(*)  then 'returned'
            when countif(is_returned) > 0         then 'partially_returned'
            when countif(order_item_status = 'complete') = count(*) then 'complete'
            else 'in_progress'
        end                                               as order_status

    from order_items
    group by order_id, user_id

)

select * from aggregated
