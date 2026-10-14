-- Raw / Bronze: 1 linha por item de pedido. Só renomeia, tipa e padroniza; sem regra de negócio.
with source as (

    select * from {{ source('thelook_ecommerce', 'order_items') }}

),

renamed as (

    select
        -- chaves
        cast(id as int64)                as order_item_id,
        cast(order_id as int64)          as order_id,
        cast(user_id as int64)           as user_id,
        cast(product_id as int64)        as product_id,
        cast(inventory_item_id as int64) as inventory_item_id,

        -- atributos
        lower(trim(status))              as order_item_status,

        -- medidas
        cast(sale_price as numeric)      as sale_price,

        -- timestamps
        created_at                       as ordered_at,
        shipped_at,
        delivered_at,
        returned_at

    from source
    where id is not null
    {{ limit_data_in_dev('created_at') }}

)

select * from renamed
