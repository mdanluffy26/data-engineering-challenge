"""ETL das corridas de táxi amarelo de Nova York (2022) para o PostgreSQL.

Fluxo:
    create_schemas -> list_files -> load_raw (1 por mês) -> load_trusted (1 por mês)
        -> build_refined -> answer_questions

Camadas no banco ``nyc_taxi`` (conexão ``nyc_taxi_dw``):
    raw.yellow_tripdata      dados como vieram do arquivo
    trusted.yellow_tripdata  dados tipados e limpos
    refined.trips            tabela final para análise (+ refined.daily_trips)
"""

from __future__ import annotations

import io
import logging
import os
import re
from datetime import datetime
from pathlib import Path

import pendulum
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

CONN_ID = "nyc_taxi_dw"
DATA_DIR = Path(os.environ.get("NYC_TLC_DATA_DIR", "/opt/airflow/data"))
SQL_DIR = Path(__file__).parent / "sql"
QUERIES_DIR = Path(os.environ.get("QUERIES_DIR", "/opt/airflow/queries"))
BATCH_SIZE = 250_000

# Ordem das colunas na tabela raw (nomes do arquivo em minúsculas)
RAW_COLUMNS = [
    "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "ratecodeid", "store_and_fwd_flag", "pulocationid", "dolocationid",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
]
FILE_PATTERN = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet(\.gz)?$")


def month_bounds(source_file: str) -> tuple[datetime, datetime]:
    """Primeiro instante do mês do arquivo e do mês seguinte."""
    year, month = map(int, FILE_PATTERN.search(source_file).groups()[:2])
    start = datetime(year, month, 1)
    end = datetime(year + month // 12, month % 12 + 1, 1)
    return start, end


def open_parquet(path: Path) -> pq.ParquetFile:
    """Abre o parquet tratando os dois casos: arquivo .gz de verdade ou só com a extensão.

    Os arquivos do desafio têm extensão .parquet.gz, mas começam com ``PAR1``
    (parquet puro, sem gzip). Por isso decidimos pelo conteúdo, não pelo nome.
    """
    with open(path, "rb") as f:
        magic = f.read(4)
    if magic == b"PAR1":
        return pq.ParquetFile(path)
    if magic[:2] == b"\x1f\x8b":
        import gzip

        with gzip.open(path, "rb") as g:
            return pq.ParquetFile(pa.BufferReader(g.read()))
    raise ValueError(f"{path.name}: formato desconhecido (bytes iniciais {magic!r})")


def run_sql_file(name: str, parameters: dict | None = None) -> None:
    sql = (SQL_DIR / name).read_text(encoding="utf-8")
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, parameters)


@dag(
    dag_id="nyc_taxi_etl",
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["nyc-taxi", "etl"],
    doc_md=__doc__,
)
def nyc_taxi_etl():
    @task
    def create_schemas() -> None:
        run_sql_file("00_create_schemas.sql")

    @task
    def list_files() -> list[str]:
        files = sorted(p.name for p in DATA_DIR.iterdir() if FILE_PATTERN.search(p.name))
        if not files:
            raise FileNotFoundError(f"Nenhum arquivo yellow_tripdata em {DATA_DIR}")
        log.info("%d arquivos encontrados: %s", len(files), files)
        return files

    @task(max_active_tis_per_dag=3)
    def load_raw(source_file: str) -> str:
        """Extract + Load: lê o parquet em lotes e grava na raw via COPY (idempotente por arquivo)."""
        parquet = open_parquet(DATA_DIR / source_file)
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        copy_sql = (
            f"COPY raw.yellow_tripdata ({', '.join(RAW_COLUMNS)}, source_file) "
            "FROM STDIN WITH (FORMAT csv, HEADER true)"
        )
        rows = 0
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM raw.yellow_tripdata WHERE source_file = %s", (source_file,))
            for batch in parquet.iter_batches(batch_size=BATCH_SIZE):
                table = pa.Table.from_batches([batch])
                table = table.rename_columns([c.lower() for c in table.column_names])
                missing = set(RAW_COLUMNS) - set(table.column_names)
                if missing:
                    raise ValueError(f"{source_file}: colunas ausentes {sorted(missing)}")
                table = table.select(RAW_COLUMNS).append_column(
                    "source_file", pa.array([source_file] * table.num_rows, pa.string())
                )
                buffer = io.BytesIO()
                pa_csv.write_csv(table, buffer)
                buffer.seek(0)
                cur.copy_expert(copy_sql, buffer)
                rows += table.num_rows
        log.info("%s: %d linhas carregadas na raw", source_file, rows)
        return source_file

    @task(max_active_tis_per_dag=3)
    def load_trusted(source_file: str) -> None:
        """Transform: tipagem, renomeação e regras de qualidade."""
        start, end = month_bounds(source_file)
        run_sql_file(
            "10_load_trusted.sql",
            {"source_file": source_file, "month_start": start, "month_end": end},
        )
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        raw_rows, trusted_rows = hook.get_first(
            "SELECT raw_rows, trusted_rows FROM trusted.load_audit WHERE source_file = %s",
            parameters=(source_file,),
        )
        log.info(
            "%s: raw=%d trusted=%d descartadas=%d (%.2f%%)",
            source_file, raw_rows, trusted_rows, raw_rows - trusted_rows,
            100 * (raw_rows - trusted_rows) / raw_rows,
        )

    @task
    def build_refined() -> None:
        run_sql_file("20_build_refined.sql")

    @task
    def answer_questions() -> dict[str, list]:
        """Executa as queries das perguntas do desafio e registra as respostas no log."""
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        answers = {}
        for path in sorted(QUERIES_DIR.glob("q*.sql")):
            with hook.get_conn() as conn, conn.cursor() as cur:
                cur.execute(path.read_text(encoding="utf-8"))
                columns = [d.name for d in cur.description]
                rows = [dict(zip(columns, map(str, row))) for row in cur.fetchall()]
            answers[path.stem] = rows
            log.info("%s -> %s", path.name, rows)
        return answers

    files = list_files()
    raw_loaded = load_raw.expand(source_file=files)
    create_schemas() >> files
    load_trusted.expand(source_file=raw_loaded) >> build_refined() >> answer_questions()


nyc_taxi_etl()
