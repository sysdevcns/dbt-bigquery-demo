-- Raw / Bronze: catálogo de produtos.
with source as (

    select * from {{ source('thelook_ecommerce', 'products') }}

),

renamed as (

    select
        cast(id as int64)                     as product_id,
        cast(distribution_center_id as int64) as distribution_center_id,
        trim(sku)                             as sku,
        trim(name)                            as product_name,
        trim(brand)                           as brand,
        trim(category)                        as category,
        trim(department)                      as department,
        cast(cost as numeric)                 as cost,
        cast(retail_price as numeric)         as retail_price

    from source
    where id is not null

)

select * from renamed
