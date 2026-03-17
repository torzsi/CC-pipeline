from __future__ import annotations

from dataclasses import dataclass
import gzip
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import duckdb

from .candidates import CCIndexEntry


DEFAULT_COLUMNS = (
    "url",
    "CAST(fetch_time AS VARCHAR) AS fetch_time",
    "content_mime_detected",
    "fetch_status",
    "content_languages",
    "content_charset",
    "warc_filename",
    "warc_record_offset",
    "warc_record_length",
)
DEFAULT_HTTP_PREFIX = "https://data.commoncrawl.org/"


@dataclass
class ColumnarQueryResult:
    rows: list[dict[str, Any]]
    sql: str
    parquet_paths: list[str]

    def to_index_entries(self) -> list[CCIndexEntry]:
        entries: list[CCIndexEntry] = []
        for row in self.rows:
            entries.append(
                CCIndexEntry(
                    url=str(row.get("url") or ""),
                    mime=row.get("content_mime_detected"),
                    status=row.get("fetch_status"),
                    length=_coerce_int(row.get("warc_record_length")),
                    filename=row.get("warc_filename"),
                    offset=_coerce_int(row.get("warc_record_offset")),
                    languages=row.get("content_languages"),
                    timestamp=row.get("fetch_time"),
                    charset=row.get("content_charset"),
                )
            )
        return entries


class ColumnarIndexClient:
    def __init__(self, *, http_prefix: str = DEFAULT_HTTP_PREFIX) -> None:
        self.http_prefix = http_prefix.rstrip("/") + "/"

    def manifest_url(self, crawl: str) -> str:
        return f"{self.http_prefix}crawl-data/{crawl}/cc-index-table.paths.gz"

    def list_parquet_paths(self, crawl: str, *, subset: str = "warc") -> list[str]:
        with urlopen(self.manifest_url(crawl)) as response:
            payload = response.read()
        with gzip.GzipFile(fileobj=BytesIO(payload)) as handle:
            lines = handle.read().decode("utf-8").splitlines()
        subset_marker = f"/subset={subset}/"
        return [
            self.http_prefix + line.lstrip("/")
            for line in lines
            if line.strip() and subset_marker in line
        ]

    def query(
        self,
        *,
        crawl: str,
        where_sql: str,
        limit: int = 10,
        columns: tuple[str, ...] = DEFAULT_COLUMNS,
        path_limit: int | None = None,
        subset: str = "warc",
    ) -> ColumnarQueryResult:
        parquet_paths = self.list_parquet_paths(crawl, subset=subset)
        if path_limit is not None:
            parquet_paths = parquet_paths[:path_limit]
        if not parquet_paths:
            return ColumnarQueryResult(rows=[], sql="", parquet_paths=[])

        conn = duckdb.connect()
        self._load_httpfs(conn)
        columns_sql = ", ".join(columns)
        parquet_list = ", ".join(_sql_quote(path) for path in parquet_paths)
        sql = (
            f"SELECT {columns_sql} "
            f"FROM read_parquet([{parquet_list}], union_by_name=true) "
            f"WHERE {where_sql} "
            f"LIMIT {int(limit)}"
        )
        cursor = conn.execute(sql)
        names = [column[0] for column in cursor.description]
        rows = [dict(zip(names, values)) for values in cursor.fetchall()]
        conn.close()
        return ColumnarQueryResult(rows=rows, sql=sql, parquet_paths=parquet_paths)

    def query_parquet_paths(
        self,
        *,
        parquet_paths: list[str],
        where_sql: str,
        limit: int | None = None,
        columns: tuple[str, ...] = DEFAULT_COLUMNS,
    ) -> ColumnarQueryResult:
        if not parquet_paths:
            return ColumnarQueryResult(rows=[], sql="", parquet_paths=[])

        conn = duckdb.connect()
        self._load_httpfs(conn)
        columns_sql = ", ".join(columns)
        parquet_list = ", ".join(_sql_quote(path) for path in parquet_paths)
        sql = (
            f"SELECT {columns_sql} "
            f"FROM read_parquet([{parquet_list}], union_by_name=true) "
            f"WHERE {where_sql}"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        cursor = conn.execute(sql)
        names = [column[0] for column in cursor.description]
        rows = [dict(zip(names, values)) for values in cursor.fetchall()]
        conn.close()
        return ColumnarQueryResult(rows=rows, sql=sql, parquet_paths=parquet_paths)

    def find_html_candidates(
        self,
        *,
        crawl: str,
        domain: str | None = None,
        host: str | None = None,
        path_prefix: str | None = None,
        url_like: str | None = None,
        limit: int = 10,
        path_limit: int | None = None,
    ) -> ColumnarQueryResult:
        where_sql = self.build_html_where_sql(
            domain=domain,
            host=host,
            path_prefix=path_prefix,
            url_like=url_like,
        )
        return self.query(crawl=crawl, where_sql=where_sql, limit=limit, path_limit=path_limit)

    def build_html_where_sql(
        self,
        *,
        domain: str | None = None,
        host: str | None = None,
        path_prefix: str | None = None,
        url_like: str | None = None,
    ) -> str:
        clauses = [
            "fetch_status = 200",
            "lower(content_mime_detected) = 'text/html'",
            "warc_filename IS NOT NULL",
            "warc_record_offset IS NOT NULL",
            "warc_record_length IS NOT NULL",
        ]
        if domain:
            escaped_domain = domain.replace("'", "''").lower()
            clauses.append(f"lower(url_host_registered_domain) = '{escaped_domain}'")
        if host:
            escaped_host = host.replace("'", "''").lower()
            clauses.append(f"lower(url_host_name) = '{escaped_host}'")
        if path_prefix:
            escaped_prefix = _sql_like_prefix(path_prefix)
            clauses.append(f"lower(url_path) LIKE '{escaped_prefix}'")
        if url_like:
            escaped_pattern = url_like.replace("'", "''").lower()
            clauses.append(f"lower(url) LIKE '{escaped_pattern}'")
        return " AND ".join(clauses)

    def iter_html_candidate_batches(
        self,
        *,
        crawl: str,
        domain: str | None = None,
        host: str | None = None,
        path_prefix: str | None = None,
        url_like: str | None = None,
        path_limit: int | None = None,
        path_batch_size: int = 1,
        rows_per_batch: int | None = None,
        subset: str = "warc",
    ):
        parquet_paths = self.list_parquet_paths(crawl, subset=subset)
        if path_limit is not None:
            parquet_paths = parquet_paths[:path_limit]

        where_sql = self.build_html_where_sql(
            domain=domain,
            host=host,
            path_prefix=path_prefix,
            url_like=url_like,
        )
        for start in range(0, len(parquet_paths), max(path_batch_size, 1)):
            batch_paths = parquet_paths[start : start + max(path_batch_size, 1)]
            yield self.query_parquet_paths(
                parquet_paths=batch_paths,
                where_sql=where_sql,
                limit=rows_per_batch,
            )

    @staticmethod
    def _load_httpfs(conn: duckdb.DuckDBPyConnection) -> None:
        try:
            conn.execute("LOAD httpfs")
        except duckdb.Error:
            conn.execute("INSTALL httpfs")
            conn.execute("LOAD httpfs")


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_like_prefix(value: str) -> str:
    escaped = value.replace("'", "''").lower()
    return escaped.replace("%", "\\%").replace("_", "\\_") + "%"


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)
