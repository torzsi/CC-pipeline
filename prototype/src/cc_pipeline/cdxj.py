from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .candidates import CCIndexEntry


DEFAULT_SERVER = "https://index.commoncrawl.org/"
DEFAULT_USER_AGENT = "cc-pipeline/0.1 (research prototype)"


@dataclass
class CDXJQueryResult:
    rows: list[dict[str, Any]]
    query_url: str

    def to_index_entries(self) -> list[CCIndexEntry]:
        entries: list[CCIndexEntry] = []
        for row in self.rows:
            entries.append(
                CCIndexEntry(
                    url=str(row.get("url") or ""),
                    mime=row.get("mime-detected") or row.get("mime"),
                    status=row.get("status"),
                    length=_coerce_int(row.get("length")),
                    filename=row.get("filename"),
                    offset=_coerce_int(row.get("offset")),
                    languages=row.get("languages"),
                    timestamp=row.get("timestamp"),
                    digest=row.get("digest"),
                    charset=row.get("encoding"),
                )
            )
        return entries


class CDXJIndexClient:
    def __init__(
        self,
        *,
        server_url: str = DEFAULT_SERVER,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: float = 30.0,
    ) -> None:
        self.server_url = server_url.rstrip("/") + "/"
        self.user_agent = user_agent
        self.timeout = timeout

    def build_target(
        self,
        *,
        url_pattern: str | None = None,
        domain: str | None = None,
        host: str | None = None,
        path_prefix: str | None = None,
        url_like: str | None = None,
        scheme: str = "https",
    ) -> str:
        if url_pattern:
            return url_pattern

        if url_like:
            return _url_like_to_cdxj_target(url_like)

        target_host = host or domain
        if not target_host:
            raise ValueError("CDXJ queries require --url-pattern, --url-like, --host, or --domain")

        prefix = path_prefix or "/"
        if not prefix.startswith("/"):
            prefix = "/" + prefix
        return f"{scheme}://{target_host}{prefix}*"

    def build_query_params(
        self,
        *,
        target: str,
        limit: int | None = None,
        filters: list[str] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"output": "json"}
        query_target = target
        if "*" in target:
            query_target = target.split("*", 1)[0]
            params["matchType"] = "prefix"
        params["url"] = query_target
        if limit is not None:
            params["limit"] = str(limit)
        if filters:
            params["filter"] = filters
        return params

    def query(
        self,
        *,
        crawl: str,
        target: str,
        limit: int | None = 10,
        filters: list[str] | None = None,
    ) -> CDXJQueryResult:
        params = self.build_query_params(target=target, limit=limit, filters=filters)
        query_url = f"{self.server_url}{crawl}-index?{urlencode(params, doseq=True)}"
        request = Request(query_url, headers={"User-Agent": self.user_agent})
        try:
            with urlopen(request, timeout=self.timeout) as response:
                payload = response.read().decode("utf-8")
        except HTTPError as exc:
            if exc.code == 404:
                return CDXJQueryResult(rows=[], query_url=query_url)
            raise

        rows = [json.loads(line) for line in payload.splitlines() if line.strip()]
        return CDXJQueryResult(rows=rows, query_url=query_url)

    def find_records(
        self,
        *,
        crawl: str,
        url_pattern: str | None = None,
        domain: str | None = None,
        host: str | None = None,
        path_prefix: str | None = None,
        url_like: str | None = None,
        limit: int | None = 10,
    ) -> CDXJQueryResult:
        target = self.build_target(
            url_pattern=url_pattern,
            domain=domain,
            host=host,
            path_prefix=path_prefix,
            url_like=url_like,
        )
        return self.query(crawl=crawl, target=target, limit=limit)


def _url_like_to_cdxj_target(url_like: str) -> str:
    target = url_like
    if target.endswith("%"):
        target = target[:-1] + "*"
    return target


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)
