import argparse
import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs, unquote

import boto3
from boto3.dynamodb.conditions import Key

TABLE_NAME = os.environ.get("DDB_TABLE")
dynamodb = boto3.resource("dynamodb")
table = None


def _resp_bytes(obj):
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def _query_recent_in_category(category, limit=20):
    resp = table.query(
        KeyConditionExpression=Key("PK").eq(f"CATEGORY#{category}"),
        ScanIndexForward=False,
        Limit=limit
    )
    return resp.get("Items", [])


def _query_papers_by_author(author_name):
    items = []
    kwargs = {
        "IndexName": "AuthorIndex",
        "KeyConditionExpression": Key("GSI1PK").eq(f"AUTHOR#{author_name}"),
        "ScanIndexForward": False
    }
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return items


def _get_paper_by_id(arxiv_id):
    resp = table.query(
        IndexName="PaperIdIndex",
        KeyConditionExpression=Key("GSI3PK").eq(f"PAPER#{arxiv_id}")
    )
    items = resp.get("Items", [])
    return items[0] if items else None


def _query_papers_in_date_range(category, start_date, end_date):
    items = []
    kwargs = {
        "KeyConditionExpression": (
            Key("PK").eq(f"CATEGORY#{category}") &
            Key("SK").between(f"{start_date}#", f"{end_date}#zzzzzzz")
        ),
        "ScanIndexForward": True
    }
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return items


def _query_papers_by_keyword(keyword, limit=20):
    resp = table.query(
        IndexName="KeywordIndex",
        KeyConditionExpression=Key("GSI2PK").eq(f"KW#{keyword.lower()}"),
        ScanIndexForward=False,
        Limit=limit
    )
    return resp.get("Items", [])


class Handler(BaseHTTPRequestHandler):
    def log_request_stdout(self, status, start_ts, extra=None):
        dur_ms = int((time.time() - start_ts) * 1000)
        info = {
            "method": self.command,
            "path": self.path,
            "status": status,
            "duration_ms": dur_ms
        }
        if extra:
            info.update(extra)
        print(json.dumps(info, ensure_ascii=False))

    def _send(self, status, payload):
        body = _resp_bytes(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        start_ts = time.time()
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)

            if path == "/papers/recent":
                category = qs.get("category", [None])[0]
                limit = qs.get("limit", [None])[0]
                if not category:
                    self._send(400, {"error": "missing category"})
                    self.log_request_stdout(400, start_ts)
                    return
                try:
                    limit = int(limit) if limit is not None else 20
                except ValueError:
                    self._send(400, {"error": "invalid limit"})
                    self.log_request_stdout(400, start_ts)
                    return
                items = _query_recent_in_category(category, limit=limit)
                self._send(200, {"category": category, "papers": items, "count": len(items)})
                self.log_request_stdout(200, start_ts, {"category": category, "limit": limit})
                return

            if path.startswith("/papers/author/"):
                author_name = unquote(path.split("/papers/author/", 1)[1])
                if not author_name:
                    self._send(400, {"error": "missing author_name"})
                    self.log_request_stdout(400, start_ts)
                    return
                items = _query_papers_by_author(author_name)
                self._send(200, {"author_name": author_name, "papers": items, "count": len(items)})
                self.log_request_stdout(200, start_ts, {"author_name": author_name})
                return

            if path.startswith("/papers/keyword/"):
                keyword = unquote(path.split("/papers/keyword/", 1)[1])
                limit = qs.get("limit", [None])[0]
                try:
                    limit = int(limit) if limit is not None else 20
                except ValueError:
                    self._send(400, {"error": "invalid limit"})
                    self.log_request_stdout(400, start_ts)
                    return
                if not keyword:
                    self._send(400, {"error": "missing keyword"})
                    self.log_request_stdout(400, start_ts)
                    return
                items = _query_papers_by_keyword(keyword, limit=limit)
                self._send(200, {"keyword": keyword, "papers": items, "count": len(items)})
                self.log_request_stdout(200, start_ts, {"keyword": keyword, "limit": limit})
                return

            if path.startswith("/papers/search"):
                category = qs.get("category", [None])[0]
                start_date = qs.get("start", [None])[0]
                end_date = qs.get("end", [None])[0]
                if not category or not start_date or not end_date:
                    self._send(400, {"error": "missing category/start/end"})
                    self.log_request_stdout(400, start_ts)
                    return
                items = _query_papers_in_date_range(category, start_date, end_date)
                self._send(200, {
                    "category": category,
                    "start": start_date,
                    "end": end_date,
                    "papers": items,
                    "count": len(items)
                })
                self.log_request_stdout(200, start_ts, {"category": category, "start": start_date, "end": end_date})
                return

            if path.startswith("/papers/"):
                arxiv_id = path.split("/papers/", 1)[1]
                if not arxiv_id:
                    self._send(400, {"error": "missing arxiv_id"})
                    self.log_request_stdout(400, start_ts)
                    return
                arxiv_id = unquote(arxiv_id)
                item = _get_paper_by_id(arxiv_id)
                if not item:
                    self._send(404, {"error": "not found"})
                    self.log_request_stdout(404, start_ts, {"arxiv_id": arxiv_id})
                    return
                self._send(200, item)
                self.log_request_stdout(200, start_ts, {"arxiv_id": arxiv_id})
                return

            self._send(404, {"error": "not found"})
            self.log_request_stdout(404, start_ts)
        except Exception as e:
            self._send(500, {"error": "server error"})
            self.log_request_stdout(500, start_ts, {"exception": str(e)})


def main():
    global table
    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("positional_port", nargs="?", type=int, help="optional positional port number")
    args = parser.parse_args()

    port = args.port if args.port is not None else (args.positional_port or 8080)

    if not TABLE_NAME:
        raise SystemExit("Missing table name: set env DDB_TABLE to your DynamoDB table.")
    table = dynamodb.Table(TABLE_NAME)

    server = HTTPServer(("0.0.0.0", port), Handler)
    print(json.dumps({"event": "server_start", "port": port, "table": TABLE_NAME}, ensure_ascii=False))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        print(json.dumps({"event": "server_stop"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
