from __future__ import annotations

from dataclasses import dataclass
from time import sleep
from typing import Any

from .candidates import CCIndexEntry


DEFAULT_TABLE = "ccindex.ccindex"
DEFAULT_OUTPUT_FORMAT = "json"
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


@dataclass
class AthenaQueryRequest:
    crawl: str
    output_location: str
    table: str = DEFAULT_TABLE
    domain: str | None = None
    limit: int | None = None
    include_pdf: bool = False
    workgroup: str = "primary"
    database: str | None = None


@dataclass
class AthenaQueryPlan:
    sql: str
    output_location: str
    workgroup: str
    database: str | None = None


@dataclass
class AthenaQueryResult:
    rows: list[dict[str, Any]]
    sql: str
    query_execution_id: str | None = None

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


class AthenaColumnarClient:
    def __init__(self, *, boto3_session: Any | None = None, poll_seconds: float = 2.0) -> None:
        self._boto3_session = boto3_session
        self.poll_seconds = poll_seconds

    def build_candidate_sql(self, request: AthenaQueryRequest) -> str:
        mime_sql = "('text/html')" if not request.include_pdf else "('text/html', 'application/pdf')"
        clauses = [
            f"crawl = '{_sql_escape(request.crawl)}'",
            "subset = 'warc'",
            "fetch_status = 200",
            f"lower(content_mime_detected) IN {mime_sql}",
            "warc_filename IS NOT NULL",
            "warc_record_offset IS NOT NULL",
            "warc_record_length IS NOT NULL",
        ]
        if request.domain:
            clauses.append(f"lower(url_host_registered_domain) = '{_sql_escape(request.domain.lower())}'")
        columns_sql = ", ".join(DEFAULT_COLUMNS)
        sql = f"SELECT {columns_sql} FROM {request.table} WHERE " + " AND ".join(clauses)
        if request.limit is not None:
            sql += f" LIMIT {int(request.limit)}"
        return sql

    def plan_candidate_query(self, request: AthenaQueryRequest) -> AthenaQueryPlan:
        return AthenaQueryPlan(
            sql=self.build_candidate_sql(request),
            output_location=request.output_location,
            workgroup=request.workgroup,
            database=request.database,
        )

    def execute_candidate_query(self, request: AthenaQueryRequest) -> AthenaQueryResult:
        client = self._athena_client()
        plan = self.plan_candidate_query(request)
        execution_args: dict[str, Any] = {
            "QueryString": plan.sql,
            "ResultConfiguration": {"OutputLocation": plan.output_location},
            "WorkGroup": plan.workgroup,
        }
        if plan.database:
            execution_args["QueryExecutionContext"] = {"Database": plan.database}

        start = client.start_query_execution(**execution_args)
        query_execution_id = start["QueryExecutionId"]
        state = self._wait_for_success(client, query_execution_id)
        if state != "SUCCEEDED":
            raise RuntimeError(f"Athena query failed with state={state}")

        rows = self._read_rows(client, query_execution_id)
        return AthenaQueryResult(rows=rows, sql=plan.sql, query_execution_id=query_execution_id)

    def _wait_for_success(self, client: Any, query_execution_id: str) -> str:
        while True:
            response = client.get_query_execution(QueryExecutionId=query_execution_id)
            state = response["QueryExecution"]["Status"]["State"]
            if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                return state
            sleep(self.poll_seconds)

    def _read_rows(self, client: Any, query_execution_id: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        next_token: str | None = None
        column_names: list[str] | None = None

        while True:
            kwargs = {"QueryExecutionId": query_execution_id}
            if next_token:
                kwargs["NextToken"] = next_token
            response = client.get_query_results(**kwargs)
            result_set = response["ResultSet"]
            metadata = result_set["ResultSetMetadata"]["ColumnInfo"]
            if column_names is None:
                column_names = [column["Name"] for column in metadata]

            page_rows = result_set["Rows"]
            if page_rows:
                start_index = 1 if column_names and _is_header_row(page_rows[0], column_names) and not rows else 0
                for row in page_rows[start_index:]:
                    data = row.get("Data", [])
                    payload: dict[str, Any] = {}
                    for index, name in enumerate(column_names):
                        value = data[index].get("VarCharValue") if index < len(data) else None
                        payload[name] = value
                    rows.append(payload)

            next_token = response.get("NextToken")
            if not next_token:
                return rows

    def _athena_client(self) -> Any:
        if self._boto3_session is not None:
            return self._boto3_session.client("athena")
        try:
            import boto3
        except ModuleNotFoundError as exc:
            raise RuntimeError("boto3 is required for Athena execution; install the aws extra") from exc
        return boto3.client("athena")


def _is_header_row(row: dict[str, Any], column_names: list[str]) -> bool:
    values = [item.get("VarCharValue") for item in row.get("Data", [])]
    return values == column_names


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)
