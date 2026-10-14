-- Teste singular: nenhum pedido deveria ter data no futuro.
--
-- Severidade warn de propósito: o dataset público thelook_ecommerce é sintético e o
-- gerador cria pedidos com created_at alguns dias à frente da data atual. Em uma fonte
-- real isso seria erro (relógio do sistema, fuso ou carga adiantada) e a severidade
-- voltaria para error. O aviso mantém a anomalia visível a cada execução sem bloquear
-- o pipeline por um comportamento conhecido da fonte.
{{ config(severity='warn') }}

select
    order_item_id,
    order_date
from {{ ref('fct_order_items') }}
where order_date > current_date()
