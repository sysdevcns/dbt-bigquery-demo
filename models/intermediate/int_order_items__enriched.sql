-- Cleansed / Silver: item de pedido enriquecido com produto e com as regras de negócio
-- de reconhecimento de receita e margem. Grão: 1 linha por item de pedido.
with order_items as (

    select * from {{ ref('stg_thelook__order_items') }}

),

products as (

    select * from {{ ref('stg_thelook__products') }}

),

enriched as (

    select
        -- chaves
        order_items.order_item_id,
        order_items.order_id,
        order_items.user_id,
        order_items.product_id,

        -- datas
        order_items.ordered_at,
        date(order_items.ordered_at)                                 as order_date,
        order_items.shipped_at,
        order_items.delivered_at,
        order_items.returned_at,
        date_diff(date(order_items.shipped_at), date(order_items.ordered_at), day)   as days_to_ship,
        date_diff(date(order_items.delivered_at), date(order_items.ordered_at), day) as days_to_deliver,

        -- atributos de produto (desnormalizados para facilitar o consumo)
        products.product_name,
        products.brand                                               as product_brand,
        products.category                                            as product_category,
        products.department                                          as product_department,

        -- status e flags de negócio
        order_items.order_item_status,
        order_items.order_item_status = 'cancelled'                  as is_cancelled,
        order_items.order_item_status = 'returned'                   as is_returned,
        order_items.order_item_status not in ('cancelled', 'returned') as is_valid_sale,

        -- medidas financeiras
        order_items.sale_price,
        products.cost                                                as product_cost,
        products.retail_price                                        as product_retail_price,
        -- Regra: receita líquida só é reconhecida quando o item não foi cancelado nem devolvido.
        case
            when order_items.order_item_status not in ('cancelled', 'returned') then order_items.sale_price
            else 0
        end                                                          as net_revenue,
        order_items.sale_price - products.cost                       as gross_margin,
        safe_divide(order_items.sale_price - products.cost, order_items.sale_price) as gross_margin_pct,
        order_items.sale_price - products.retail_price               as discount_amount

    from order_items
    left join products
        on order_items.product_id = products.product_id

)

select * from enriched
