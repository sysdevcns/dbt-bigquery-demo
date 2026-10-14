-- Exemplo de consulta analítica sobre o mart financeiro (compilada com `dbt compile`,
-- não materializada). Mostra como o particionamento por order_date reduz bytes lidos.
select
    product_category,
    sum(net_revenue)   as net_revenue,
    sum(gross_margin)  as gross_margin,
    sum(items_sold)    as items_sold
from {{ ref('mart_daily_revenue') }}
where order_date >= date_sub(current_date(), interval 90 day)
group by product_category
order by net_revenue desc
limit 10
