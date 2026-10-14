-- Raw / Bronze: cadastro de clientes com tipos e textos padronizados.
with source as (

    select * from {{ source('thelook_ecommerce', 'users') }}

),

renamed as (

    select
        cast(id as int64)                        as user_id,
        trim(first_name)                         as first_name,
        trim(last_name)                          as last_name,
        lower(trim(email))                       as email,
        cast(age as int64)                       as age,
        lower(trim(gender))                      as gender,
        trim(state)                              as state,
        trim(city)                               as city,
        {{ standardize_country('country') }}     as country,
        lower(trim(traffic_source))              as traffic_source,
        created_at                               as registered_at

    from source
    where id is not null

)

select * from renamed
