# Solução — Data Engineering Challenge

Pipeline de **ETL no Apache Airflow** que carrega as corridas de táxi amarelo de Nova York (jan–dez/2022)
em um **PostgreSQL**, organizado nas camadas **raw → trusted → refined**, e responde às perguntas do desafio em SQL.

## Arquitetura

```
nyc-tlc-data/*.parquet.gz
        │  (montado read-only no Airflow)
        ▼
┌──────────────────────── Airflow (LocalExecutor) ────────────────────────┐
│ create_schemas → list_files → load_raw[12] → load_trusted[12]           │
│                                   → build_refined → answer_questions    │
└──────────────────────────────────────────────────────────────────────────┘
        │
        ▼
PostgreSQL "nyc_taxi" (container postgres-dw)
  raw.yellow_tripdata       dados como vieram do arquivo + source_file/loaded_at
  trusted.yellow_tripdata   tipado, renomeado, filtrado pelas regras de qualidade
  trusted.load_audit        linhas por arquivo em raw x trusted
  refined.trips             TABELA FINAL (com data, duração e velocidade média)
  refined.daily_trips       agregado diário
```

| Serviço | Função | Porta |
|---|---|---|
| `airflow-webserver` | UI do Airflow | http://localhost:8080 (airflow / airflow) |
| `airflow-scheduler` | executa as tarefas | — |
| `postgres` | metadados do Airflow | — |
| `postgres-dw` | banco do desafio | `localhost:5433` (dw / dw, banco `nyc_taxi`) |

## Estrutura

```
challenge/
├── docker-compose.yaml      Airflow + 2 PostgreSQL
├── Dockerfile               imagem do Airflow + pyarrow + provider postgres
├── requirements.txt
├── dags/
│   ├── nyc_taxi_etl.py      DAG do ETL
│   └── sql/
│       ├── 00_create_schemas.sql
│       ├── 10_load_trusted.sql
│       └── 20_build_refined.sql
└── queries/                 queries das perguntas (q1…q4)
```

## Como executar

Pré-requisitos: Docker Desktop (com WSL2 no Windows), **~8 GB de RAM** para o Docker e **~25 GB de disco livre**
(são ~40 milhões de linhas em cada camada).

```bash
cd challenge
docker compose up -d --build        # 1ª vez: build da imagem + migração do Airflow
```

1. Acesse http://localhost:8080 (usuário `airflow`, senha `airflow`).
2. Ative e dispare a DAG **`nyc_taxi_etl`** (botão ▶).
3. Ao final, as respostas aparecem no log da tarefa `answer_questions` — ou rode as queries direto no banco:

```bash
docker compose exec postgres-dw psql -U dw -d nyc_taxi -f /queries/q1_total_registros.sql
docker compose exec postgres-dw psql -U dw -d nyc_taxi -f /queries/q2_viagens_17_junho.sql
docker compose exec postgres-dw psql -U dw -d nyc_taxi -f /queries/q3_dia_viagem_mais_longa.sql
docker compose exec postgres-dw psql -U dw -d nyc_taxi -f /queries/q4_estatisticas_distancia.sql
```

> No **Git Bash** (Windows), use `MSYS_NO_PATHCONV=1 docker compose exec ...` para o caminho `/queries/...`
> não ser convertido para um caminho do Windows. No PowerShell os comandos funcionam como estão.

Para derrubar tudo (incluindo os dados): `docker compose down -v`.

### Execução de referência

Ryzen 5 5600 / 16 GB para o Docker (WSL2): a DAG completa levou **~40 minutos** e o banco `nyc_taxi`
ficou com **23 GB** (raw + trusted + refined).

## Passos do ETL

1. **create_schemas** — cria os schemas `raw`, `trusted`, `refined` e as tabelas (`IF NOT EXISTS`).
2. **list_files** — lista os arquivos `yellow_tripdata_AAAA-MM.parquet[.gz]` do diretório de dados.
   As tarefas seguintes são criadas dinamicamente (*dynamic task mapping*), uma por mês.
3. **load_raw** (Extract + Load) — lê o parquet **em lotes de 250 mil linhas** (pyarrow), converte cada lote
   para CSV em memória e grava com `COPY FROM STDIN`. É idempotente: antes apaga as linhas do mesmo `source_file`.
4. **load_trusted** (Transform) — SQL que tipa, renomeia e filtra os dados do mês (regras abaixo) e grava
   a contagem em `trusted.load_audit`.
5. **build_refined** — recria `refined.trips` (tabela final, com `pickup_date`, `dropoff_date`,
   `trip_duration_min`, `avg_speed_mph`) e o agregado `refined.daily_trips`, com índices.
6. **answer_questions** — executa as queries de `queries/` e registra as respostas no log.

## Decisões tomadas

- **Os arquivos `.parquet.gz` não estão comprimidos.** Os 12 arquivos começam com os bytes `PAR1` (parquet puro);
  tentar abri-los com gzip falha (`BadGzipFile`). O código decide pelo **conteúdo** (bytes iniciais), não pela
  extensão — funciona tanto com o parquet puro quanto com um `.gz` de verdade.
- **Carga com `COPY` em lotes** em vez de `INSERT`/`pandas.to_sql`: muito mais rápido para ~40 milhões de linhas
  e com memória controlada (não carrega o mês inteiro no pandas). No máximo 3 meses carregam em paralelo.
- **Camada raw sem nenhuma transformação** (só nomes de colunas em minúsculas + `source_file` e `loaded_at`),
  para permitir reprocessar a trusted sem reler os arquivos e auditar o que foi descartado.
- **Idempotência**: toda tarefa pode ser reexecutada — raw/trusted apagam o arquivo antes de recarregar,
  e refined é reconstruída por completo.
- **Retries automáticos** (2 tentativas, 1 min de intervalo). Na primeira execução, uma carga raw falhou por
  uma falha transitória de DNS do Docker (`could not translate host name "postgres"`) no meio da carga pesada;
  como as tarefas são idempotentes, basta tentar de novo — e foi o que resolveu.
- **LocalExecutor** em vez do CeleryExecutor do compose oficial: menos containers (sem Redis/worker),
  suficiente para uma máquina só.
- **Dois PostgreSQL**: um para os metadados do Airflow e outro (`postgres-dw`) só para os dados, com ajustes de
  carga em massa (`shared_buffers`, `max_wal_size`, `maintenance_work_mem`).

### Regras de qualidade (raw → trusted)

A análise exploratória mostrou problemas em todos os meses:

| Problema encontrado | Exemplo | Regra aplicada |
|---|---|---|
| Embarque fora do mês do arquivo | datas de 2001, 2003, 2008 e 2023 | `pickup` dentro do mês do arquivo |
| Desembarque antes/igual ao embarque | ~2–3 mil por mês | duração `> 0` |
| Viagens de vários dias | — | duração `<= 24 h` |
| Distância zero | ~30–60 mil por mês | `trip_distance > 0` |
| Distâncias impossíveis | 389.678 milhas | velocidade média `<= 100 mph` |
| Linhas duplicadas | — | `SELECT DISTINCT` |

A regra de **velocidade** foi escolhida em vez de um teto fixo de distância: uma viagem de 300 milhas é possível
se durou horas, mas não em 20 minutos. Com ela, a maior distância passa a ser 380 milhas em ~6h45 (~56 mph).
Campos nulos (ex.: `passenger_count`, ~3% das linhas) **não** descartam a viagem, pois não afetam as perguntas.

Resultado: **39.656.098** linhas na raw → **39.018.136** na trusted/refined (**1,6%** descartadas).
O detalhe por mês fica em `trusted.load_audit`.

## Respostas

Resultados obtidos no PostgreSQL (`refined.trips`) após a execução completa da DAG — os mesmos valores
aparecem no log da tarefa `answer_questions` e coincidem com a análise exploratória feita com pyarrow.

### 1. Total de registros na tabela final

```sql
SELECT count(*) AS total_registros
FROM refined.trips;
```

**39.018.136** registros.

### 2. Viagens iniciadas e finalizadas em 17 de junho

```sql
SELECT count(*) AS viagens_17_junho
FROM refined.trips
WHERE pickup_date  = DATE '2022-06-17'
  AND dropoff_date = DATE '2022-06-17';
```

**121.476** viagens (começaram e terminaram em 17/06/2022). Considerando só o início no dia: 123.211.

### 3. Dia da viagem mais longa

```sql
SELECT pickup_date AS dia, pickup_datetime, dropoff_datetime, trip_distance, trip_duration_min
FROM refined.trips
ORDER BY trip_distance DESC, pickup_datetime
LIMIT 1;
```

**13/06/2022** — 380,13 milhas, de 01:36 às 08:21.
(Sem a limpeza, o "recorde" seria uma viagem de 389.678 milhas — um erro de registro.)

### 4. Estatísticas da distância percorrida (milhas)

```sql
SELECT ROUND(AVG(trip_distance), 4)          AS media,
       ROUND(STDDEV_SAMP(trip_distance), 4)  AS desvio_padrao,
       MIN(trip_distance)                    AS minimo,
       PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance) AS q1_25,
       PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY trip_distance) AS q2_mediana,
       PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance) AS q3_75,
       MAX(trip_distance)                    AS maximo
FROM refined.trips;
```

| Média | Desvio padrão | Mínimo | Q1 (25%) | Mediana (50%) | Q3 (75%) | Máximo |
|---|---|---|---|---|---|---|
| 3,5517 | 4,4906 | 0,01 | 1,14 | 1,90 | 3,60 | 380,13 |

A distribuição é bem assimétrica à direita: metade das viagens tem menos de 2 milhas, mas a média é puxada para
cima pelas viagens longas (aeroportos, fora da cidade).
