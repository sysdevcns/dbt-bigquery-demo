{% docs __overview__ %}
# dbt + BigQuery demo (Raw / Cleansed / Curated)

Projeto de demonstração sobre o dataset público `bigquery-public-data.thelook_ecommerce`.

| Camada dbt    | Medallion | Pasta                 | Materialização | Papel |
|---------------|-----------|-----------------------|----------------|-------|
| sources       | Bronze    | `_thelook__sources.yml` | (fonte)      | Dado bruto, lido direto da origem |
| staging       | Bronze    | `models/staging`      | view           | Renomeia, tipa, padroniza |
| intermediate  | Silver    | `models/intermediate` | view           | Regras de negócio, joins, agregações |
| marts         | Gold      | `models/marts`        | table / incremental | Fatos, dimensões e agregados para consumo |

Veja o README do repositório para instruções de execução.
{% enddocs %}
