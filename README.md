# dbt + BigQuery demo: pipeline em camadas Raw / Cleansed / Curated

Projeto de demonstração de engenharia de dados analítica com **dbt Core** sobre o **Google BigQuery**,
usando o dataset público `bigquery-public-data.thelook_ecommerce` (e-commerce sintético com pedidos,
itens, clientes e produtos). Roda inteiro no **sandbox gratuito do BigQuery**, sem cartão de crédito.

O objetivo é mostrar, de ponta a ponta, as práticas que uso em um pipeline analítico:

- arquitetura em camadas (Raw -> Cleansed -> Curated), equivalente ao padrão Medallion (Bronze/Silver/Gold);
- modelagem dimensional na camada de consumo (fato, dimensão e agregado financeiro);
- **particionamento e clustering** nas tabelas de marts, com a justificativa de cada escolha;
- modelo **incremental** com `insert_overwrite` por partição, idempotente e compatível com o sandbox;
- **testes de qualidade** genéricos (`unique`, `not_null`, `relationships`, `accepted_values`, `dbt_utils`),
  um teste genérico customizado e testes singulares de reconciliação;
- documentação gerada pelo dbt, seeds, macros, separação dev/prod e CI com `dbt parse`.

---

## Sumário

1. [Arquitetura](#1-arquitetura)
2. [Mapeamento Medallion x camadas dbt](#2-mapeamento-medallion-x-camadas-dbt)
3. [Modelos](#3-modelos)
4. [Decisões de modelagem física](#4-decisões-de-modelagem-física)
5. [Testes de qualidade](#5-testes-de-qualidade)
6. [Pré-requisitos](#6-pré-requisitos)
7. [Configuração do BigQuery sandbox e do profiles.yml](#7-configuração-do-bigquery-sandbox-e-do-profilesyml)
8. [Como rodar](#8-como-rodar)
9. [Estrutura do repositório](#9-estrutura-do-repositório)
10. [Limitações do sandbox](#10-limitações-do-sandbox)
11. [Próximos passos](#11-próximos-passos)

---

## 1. Arquitetura

```
 bigquery-public-data.thelook_ecommerce            (fonte: dado bruto, nunca copiado)
 ├── order_items ─┐
 ├── users ───────┤  sources (freshness + testes de PK na origem)
 └── products ────┘
        │
        ▼  RAW / BRONZE  ── views ── dataset <alvo>_staging
 stg_thelook__order_items   stg_thelook__users   stg_thelook__products
   (renomeia, tipa, padroniza status/país/e-mail; filtro de volume em dev)
        │
        ▼  CLEANSED / SILVER  ── views ── dataset <alvo>_intermediate
 int_order_items__enriched  (join item x produto, receita líquida, margem, flags)
 int_orders__aggregated     (item -> pedido, status consolidado do pedido)
        │
        ▼  CURATED / GOLD  ── tabelas particionadas e clusterizadas ── dataset <alvo>_marts
 fct_order_items    (fato, incremental, partição order_date/dia, cluster status+categoria)
 dim_customers      (dimensão, partição mês de cadastro, cluster país+segmento)  <-- seed country_regions
 mart_daily_revenue (agregado financeiro diário, partição order_date, cluster depto+categoria)
        │
        ▼
 BI / Finanças / Data Science  (Looker Studio, Power BI, notebooks)
```

Lineage real gerada pelo dbt: `dbt docs generate` seguido de `dbt docs serve`.

Princípios aplicados:

| Princípio | Como aparece no projeto |
|---|---|
| Fonte única de verdade | Toda receita vem de `fct_order_items`; `mart_daily_revenue` é derivado dela e um teste reconcilia os dois. |
| Regra de negócio em um só lugar | Reconhecimento de receita e margem vivem em `int_order_items__enriched`, nunca nos marts. |
| Staging "burro" | Staging só renomeia, tipa e padroniza. Sem joins, sem agregação. |
| Idempotência | O incremental reescreve partições inteiras; rodar duas vezes gera o mesmo resultado. |
| Ambientes isolados | `dev` lê 365 dias e grava em datasets com seu prefixo; `prod` lê tudo. |
| Qualidade como código | Mais de 60 testes declarados em YAML rodam em todo `dbt build`. |

## 2. Mapeamento Medallion x camadas dbt

| Medallion (Databricks) | Nome neste projeto | Camada dbt | Pasta | Materialização | O que acontece |
|---|---|---|---|---|---|
| **Bronze** | **Raw** | `sources` + `staging` | `models/staging/` | view | Dado como chega da origem. O dbt declara as tabelas fonte (com freshness e testes de chave) e cria views finas que só renomeiam colunas, convertem tipos e padronizam textos (`status` em minúsculas, país normalizado, e-mail em minúsculas). Nenhuma regra de negócio. |
| **Silver** | **Cleansed** | `intermediate` | `models/intermediate/` | view | Dado limpo e integrado. Joins entre entidades, regras de negócio (o que conta como receita, margem bruta, status consolidado do pedido), flags e métricas derivadas. Ainda no grão operacional, reutilizável por vários marts. |
| **Gold** | **Curated** | `marts` | `models/marts/` | table / incremental | Dado pronto para consumo. Modelo dimensional (fato + dimensão) e agregados por domínio (`finance`), com particionamento e clustering pensados para o padrão de consulta do BI. |

Observações que costumam cair em entrevista:

- Em BigQuery não faz sentido "copiar" o Bronze: as fontes já estão em tabelas colunares, então a camada Raw
  é composta por `sources` + views de staging. Em um lake (GCS + Parquet) o Bronze seria a ingestão física.
- Staging como **view** evita custo de armazenamento e garante que nunca fique defasada. Intermediate também é
  view aqui por ser leve; em volume maior viraria `ephemeral` (inlined no mart) ou `table` com partição.
- Marts são **tabelas** porque são lidas muitas vezes por dia por ferramentas de BI: materializar uma vez e
  servir barato é melhor do que recalcular a cada consulta.

## 3. Modelos

### Raw / Bronze: `models/staging/thelook/`

| Modelo | Grão | Destaques |
|---|---|---|
| `stg_thelook__order_items` | 1 linha por item de pedido | Cast de tipos, `status` em minúsculas, macro `limit_data_in_dev` (lê só 365 dias em dev). |
| `stg_thelook__users` | 1 linha por cliente | E-mail em minúsculas, macro `standardize_country` (Brasil -> Brazil, Deutschland -> Germany, España -> Spain). |
| `stg_thelook__products` | 1 linha por produto | Cast de `cost` e `retail_price` para `NUMERIC` (dinheiro nunca em float). |

### Cleansed / Silver: `models/intermediate/`

| Modelo | Grão | Destaques |
|---|---|---|
| `int_order_items__enriched` | 1 linha por item | Join com produto; `net_revenue` (só itens não cancelados/devolvidos), `gross_margin`, `gross_margin_pct`, `days_to_ship`, `days_to_deliver`, flags `is_cancelled`, `is_returned`, `is_valid_sale`. |
| `int_orders__aggregated` | 1 linha por pedido | Soma itens em pedido; deriva `order_status` consolidado (complete, in_progress, cancelled, returned, partially_returned). |

### Curated / Gold: `models/marts/`

| Modelo | Grão | Partição | Cluster | Materialização |
|---|---|---|---|---|
| `core/fct_order_items` | item de pedido | `order_date` (dia) | `order_item_status`, `product_category` | incremental (`insert_overwrite` + `copy_partitions`) |
| `core/dim_customers` | cliente | `registered_month` (mês) | `country`, `customer_segment` | table |
| `finance/mart_daily_revenue` | dia x departamento x categoria | `order_date` (dia) | `product_department`, `product_category` | table |

`dim_customers` traz métricas de ciclo de vida (primeiro/último pedido, total de pedidos, receita vitalícia,
ticket médio) e uma segmentação simples (`prospect`, `new`, `returning`, `vip`). A região geográfica vem do
seed `seeds/country_regions.csv`.

## 4. Decisões de modelagem física

### Particionamento

- **Fatos e agregados são particionados por data do pedido (dia).** Quase toda consulta financeira filtra por
  período ("últimos 90 dias", "mês fechado"). Com a partição, o BigQuery lê apenas os dias pedidos, o que
  reduz bytes processados (custo) e tempo. Como o sandbox conta bytes lidos na cota de 1 TB/mês, isso é
  literalmente a diferença entre caber ou não na cota.
- **A dimensão de clientes é particionada por mês de cadastro.** Em uma dimensão pequena (100 mil linhas)
  isso é opcional e serve mais para análises de coorte por safra de cadastro. A alternativa, igualmente
  válida, seria não particionar e só clusterizar. Regra prática: particionar quando a tabela passa de
  cerca de 1 GB ou quando há um filtro de data dominante; abaixo disso, clustering costuma bastar.
- Granularidade mensal em vez de diária na dimensão evita o limite de 4.000 partições por tabela e
  partições minúsculas (anti-padrão no BigQuery).

### Sandbox: particionamento desligado em dev

O sandbox gratuito impõe expiração de 60 dias por partição e não permite alterar isso sem faturamento.
Na primeira execução deste projeto a dimensão de clientes ficou com 5 mil linhas em vez de 100 mil e o
teste de integridade entre fato e dimensão falhou: as partições antigas foram descartadas na gravação.
A solução foi o macro `macros/is_sandbox.sql`: quando o alvo é o sandbox (padrão no target `dev`), os marts
são criados sem `partition_by` e `fct_order_items` é materializado como `table`; fora do sandbox valem as
decisões de particionamento e o incremental descritos acima. Para testar o modo completo em um projeto
com faturamento: `dbt build --profiles-dir . --vars '{bigquery_sandbox: false}'`.

### Clustering

- Depois da partição, as colunas mais usadas em `WHERE` e `GROUP BY` viram colunas de cluster. Na fato:
  status e categoria. No mart financeiro: departamento e categoria. Na dimensão: país e segmento.
- O BigQuery ordena os blocos dentro de cada partição por essas colunas, então filtros como
  `where order_item_status = 'returned'` leem só os blocos relevantes.
- A ordem importa: a primeira coluna de cluster deve ser a mais filtrada e com cardinalidade média.

### Incremental em `fct_order_items`

- Estratégia `insert_overwrite`: a cada execução, o dbt recalcula as partições dos últimos
  `incremental_lookback_days` dias (padrão 3) e as substitui inteiras. Isso captura mudanças de status
  (enviado -> entregue -> devolvido) sem precisar de `MERGE` por chave e sem duplicar linhas.
- `copy_partitions: true` faz a troca via API de cópia de partições em vez de DML. É mais barato e é o que
  permite rodar no sandbox, que **não aceita DML** (`MERGE`, `DELETE`, `UPDATE`).
- `on_schema_change: append_new_columns`: coluna nova no modelo é adicionada à tabela sem derrubar o histórico.
- Reconstrução total quando necessário: `dbt run --select fct_order_items --full-refresh`.

### Tipos e dinheiro

- Valores monetários são `NUMERIC` (decimal exato), nunca `FLOAT64`. Somas de milhões de linhas em float
  acumulam erro de arredondamento e quebram reconciliações contábeis.

## 5. Testes de qualidade

Todos rodam com `dbt test` (ou `dbt build`, que intercala run e test na ordem do DAG).

| Tipo | Exemplos neste projeto |
|---|---|
| `unique` / `not_null` | Chaves primárias em todas as camadas, inclusive nas fontes (`id` de `order_items`, `users`, `products`). |
| `relationships` | `stg_thelook__order_items.user_id -> stg_thelook__users`, `product_id -> stg_thelook__products`, `fct_order_items.user_id -> dim_customers`. |
| `accepted_values` | `order_item_status`, `order_status`, `customer_segment`, `product_department`, `gender`, `region`. |
| `dbt_utils` | `accepted_range` (preços e receita >= 0, margem % entre -5 e 1), `unique_combination_of_columns` no grão do mart diário. |
| **Teste genérico customizado** | `tests/generic/test_date_not_before.sql`: garante que `delivered_at` nunca é anterior a `shipped_at`. Reutilizável em qualquer par de colunas via YAML (`compared_to:`). Severidade `warn` porque a fonte é sintética. |
| **Testes singulares** | `tests/assert_daily_revenue_reconciles_with_fact.sql`: a receita líquida do mart diário tem que bater com a soma da fato (tolerância 0,01). `tests/assert_no_future_orders.sql`: nenhum pedido no futuro (severidade `warn`, porque o gerador do dataset sintético cria pedidos alguns dias à frente da data atual). |
| Freshness da fonte | `dbt source freshness` avisa se o `created_at` mais recente de `order_items` tiver mais de 30 dias. |

Por que isso importa: os testes são a "especificação executável" do contrato de dados. Um refactor que perca
um filtro ou duplique um join falha no CI antes de chegar ao dashboard.

## 6. Pré-requisitos

- Python 3.10 a 3.13
- Git
- [Google Cloud SDK (gcloud)](https://cloud.google.com/sdk/docs/install) para autenticação local sem chave JSON
- Uma conta Google (Gmail serve) para criar o projeto no sandbox do BigQuery

## 7. Configuração do BigQuery sandbox e do profiles.yml

### 7.1 Criar o projeto no sandbox (gratuito, sem cartão)

1. Acesse <https://console.cloud.google.com/bigquery> com a sua conta Google.
2. Crie um projeto (ex.: `dbt-demo-sidney`). Anote o **ID do projeto** (não é o nome; aparece abaixo do nome).
3. O console mostra o selo **Sandbox** no topo. Você já pode consultar `bigquery-public-data.thelook_ecommerce`
   no editor de SQL para confirmar o acesso:

   ```sql
   select count(*) from `bigquery-public-data.thelook_ecommerce.order_items`;
   ```

Cotas do sandbox: 1 TB de consulta por mês e 10 GB de armazenamento. Este projeto inteiro usa uma fração disso.

### 7.2 Autenticar localmente

```bash
gcloud auth application-default login
```

```bash
gcloud config set project SEU_PROJECT_ID
```

Isso grava as Application Default Credentials no seu usuário. O dbt usa essas credenciais com `method: oauth`.
Nenhuma chave JSON é criada nem versionada.

### 7.3 Criar o profiles.yml

```bash
cp profiles.yml.example profiles.yml
```

Edite `profiles.yml` e troque `seu-projeto-gcp` pelo ID do seu projeto. Se quiser, mude `dataset` (é o
prefixo dos datasets que o dbt vai criar: `<dataset>_staging`, `<dataset>_intermediate`, `<dataset>_marts`,
`<dataset>_seeds`). Mantenha `location: US`, porque o dataset público fica na multi-região US e o BigQuery
não faz join entre regiões.

`profiles.yml` está no `.gitignore` e **nunca** deve ser commitado. O arquivo também pode ficar em
`~/.dbt/profiles.yml`; neste projeto ele fica na raiz para facilitar, e os comandos abaixo usam `--profiles-dir .`.

Para produção, o `profiles.yml.example` já traz um target `prod` com `method: service-account`, apontando
para um `keyfile` fora do repositório. Em CI, a chave entra como secret e o caminho via variável de ambiente.

## 8. Como rodar

Criar o ambiente virtual e instalar o dbt:

```bash
python -m venv .venv
```

Windows (PowerShell):

```bash
.\.venv\Scripts\Activate.ps1
```

Linux/macOS:

```bash
source .venv/bin/activate
```

```bash
pip install -r requirements.txt
```

Instalar pacotes do dbt (`dbt_utils`) e validar a conexão:

```bash
dbt deps --profiles-dir .
```

```bash
dbt debug --profiles-dir .
```

Executar o pipeline completo (seeds, modelos e testes, na ordem do DAG):

```bash
dbt build --profiles-dir .
```

Comandos úteis no dia a dia:

```bash
dbt parse --profiles-dir .
```

```bash
dbt source freshness --profiles-dir .
```

```bash
dbt run --profiles-dir . --select tag:gold
```

```bash
dbt test --profiles-dir . --select fct_order_items+
```

```bash
dbt run --profiles-dir . --select fct_order_items --full-refresh
```

Ler todo o histórico em dev (o padrão são 365 dias):

```bash
dbt build --profiles-dir . --vars "{dev_lookback_days: 3650}"
```

Gerar e navegar a documentação com lineage:

```bash
dbt docs generate --profiles-dir .
```

```bash
dbt docs serve --profiles-dir .
```

Após o `dbt build`, no console do BigQuery você verá os datasets `<dataset>_staging`, `_intermediate`,
`_marts` e `_seeds`. Em `_marts`, abra `fct_order_items` e a aba Detalhes para ver "Particionado por
order_date" e "Clusterizado por order_item_status, product_category".

## 9. Estrutura do repositório

```
dbt-bigquery-demo/
├── dbt_project.yml              # configuração por camada (materialização, schema, tags, vars)
├── packages.yml                 # dbt_utils
├── profiles.yml.example         # modelo de conexão (copiar para profiles.yml, que é ignorado pelo git)
├── requirements.txt             # dbt-bigquery
├── .sqlfluff                    # lint de SQL (dialeto bigquery, templater dbt)
├── .github/workflows/ci.yml     # CI: dbt deps + dbt parse a cada push/PR
├── models/
│   ├── overview.md              # texto da página inicial do dbt docs
│   ├── staging/thelook/         # RAW / BRONZE
│   │   ├── _thelook__sources.yml
│   │   ├── _thelook__models.yml
│   │   ├── stg_thelook__order_items.sql
│   │   ├── stg_thelook__users.sql
│   │   └── stg_thelook__products.sql
│   ├── intermediate/            # CLEANSED / SILVER
│   │   ├── _intermediate__models.yml
│   │   ├── int_order_items__enriched.sql
│   │   └── int_orders__aggregated.sql
│   └── marts/                   # CURATED / GOLD
│       ├── _marts__models.yml
│       ├── core/
│       │   ├── fct_order_items.sql
│       │   └── dim_customers.sql
│       └── finance/
│           └── mart_daily_revenue.sql
├── macros/
│   ├── limit_data_in_dev.sql    # filtro de volume em dev
│   └── standardize_country.sql  # padronização de país
├── seeds/
│   ├── _seeds.yml
│   └── country_regions.csv      # país -> região
├── tests/
│   ├── generic/test_date_not_before.sql                 # teste genérico customizado
│   ├── assert_daily_revenue_reconciles_with_fact.sql    # teste singular (reconciliação)
│   └── assert_no_future_orders.sql                      # teste singular
└── analyses/
    └── top_categories_last_90_days.sql                  # consulta de exemplo (dbt compile)
```

Convenções: prefixos `stg_`, `int_`, `fct_`, `dim_`, `mart_`; arquivos YAML começam com `_` e ficam ao lado
dos modelos que documentam; nomes de staging seguem `stg_<fonte>__<entidade>`.

## 10. Limitações do sandbox

| Limitação do sandbox | Impacto | Como o projeto lida |
|---|---|---|
| Sem DML (`MERGE`, `UPDATE`, `DELETE`) | Estratégias incrementais `merge` e `insert_overwrite` padrão falham | `fct_order_items` usa `insert_overwrite` com `copy_partitions: true` (API de cópia, sem DML). Se preferir, troque para `materialized='table'`. |
| Partições expiram em 60 dias (não configurável no sandbox) | Em tabela particionada por data, as partições com mais de 60 dias são descartadas na própria gravação: a dimensão perderia todo cliente antigo e a fato, todo o histórico | Macro `is_sandbox()`: em `dev` os marts são materializados sem particionamento (só clustering) e `fct_order_items` vira `table` em vez de incremental. Fora do sandbox (`--vars '{bigquery_sandbox: false}'` ou target `prod`) valem partição e incremental. |
| Tabelas expiram em 60 dias | Os marts somem se o projeto ficar parado | Basta rodar `dbt build` de novo; tudo é reconstruível a partir da fonte. Em produção, remova a expiração padrão do dataset. |
| 1 TB de consulta por mês | Cota compartilhada com suas outras consultas | Marts particionados, `maximum_bytes_billed` de 2 GB por query no profile e filtro de 365 dias em dev. |
| 10 GB de armazenamento | Pouco espaço para cópias | Staging e intermediate são views; só os marts ocupam espaço (menos de 200 MB). |
| Sem streaming nem Data Transfer | Não afeta | O projeto só lê dados públicos já carregados. |

## 11. Próximos passos

Ideias de evolução que mostram maturidade de plataforma (não implementadas aqui de propósito, para manter o
escopo de uma semana):

- **Snapshots** (`snapshots/`) para SCD tipo 2 em `users` (histórico de mudança de endereço/segmento).
- **Exposures** apontando para o dashboard que consome `mart_daily_revenue`.
- **Contratos de modelo** (`contract: enforced`) e versões de modelo nos marts.
- **Slim CI** (`dbt build --select state:modified+`) rodando em um dataset efêmero por PR, usando
  service account via secret.
- **Orquestração** com Cloud Composer (Airflow) ou Cloud Run Jobs agendados; `dbt source freshness` como
  sensor antes do `dbt build`.
- **Observabilidade**: `store_failures: true` em um dataset de auditoria, elementary ou dbt artifacts.
- **Lint**: `sqlfluff lint models/` como etapa do CI (configuração já em `.sqlfluff`).

---

Autor: Sidney Moraes. Projeto criado como evidência prática de dbt + BigQuery + Git para entrevista de
engenharia/arquitetura de dados.
