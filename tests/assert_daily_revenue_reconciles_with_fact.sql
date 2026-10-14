-- Teste singular (customizado): reconciliação financeira entre camadas.
-- A receita líquida total do mart diário tem que bater com a soma da fato de itens.
-- Se divergir (ex.: filtro perdido num refactor), o teste falha e o deploy para.
with fact_total as (

    select sum(net_revenue) as net_revenue
    from {{ ref('fct_order_items') }}

),

mart_total as (

    select sum(net_revenue) as net_revenue
    from {{ ref('mart_daily_revenue') }}

)

select
    fact_total.net_revenue as fact_net_revenue,
    mart_total.net_revenue as mart_net_revenue,
    abs(fact_total.net_revenue - mart_total.net_revenue) as difference
from fact_total
cross join mart_total
where abs(coalesce(fact_total.net_revenue, 0) - coalesce(mart_total.net_revenue, 0)) > 0.01
