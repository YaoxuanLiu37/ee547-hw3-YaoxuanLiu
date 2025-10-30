import argparse
import json
import os
import time
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")

def _table(table_name):
    return dynamodb.Table(table_name)

def _json_out(payload):
    print(json.dumps(payload, ensure_ascii=False))

def _exec_timed(func, *args, **kwargs):
    t0 = time.time()
    res = func(*args, **kwargs)
    t1 = time.time()
    return res, int((t1 - t0) * 1000)

def query_recent_in_category(table_name, category, limit=20):
    """
    Query 1: Browse recent papers in category.
    Uses: Main table partition key query with sort key descending.
    """
    response = _table(table_name).query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
        ScanIndexForward=False,
        Limit=limit
    )
    return response.get('Items', [])

def query_papers_by_author(table_name, author_name):
    """
    Query 2: Find all papers by author.
    Uses: GSI1 (AuthorIndex) partition key query.
    """
    items = []
    kwargs = {
        "IndexName": "AuthorIndex",
        "KeyConditionExpression": Key('GSI1PK').eq(f'AUTHOR#{author_name}'),
        "ScanIndexForward": False
    }
    while True:
        response = _table(table_name).query(**kwargs)
        items.extend(response.get('Items', []))
        lek = response.get('LastEvaluatedKey')
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return items

def get_paper_by_id(table_name, arxiv_id):
    """
    Query 3: Get specific paper by ID.
    Uses: GSI3 (PaperIdIndex) for direct lookup.
    """
    response = _table(table_name).query(
        IndexName='PaperIdIndex',
        KeyConditionExpression=Key('GSI3PK').eq(f'PAPER#{arxiv_id}')
    )
    items = response.get('Items', [])
    return items[0] if items else None

def query_papers_in_date_range(table_name, category, start_date, end_date):
    """
    Query 4: Papers in category within date range.
    Uses: Main table with composite sort key range query.
    """
    items = []
    kwargs = {
        "KeyConditionExpression": (
            Key('PK').eq(f'CATEGORY#{category}') &
            Key('SK').between(f'{start_date}#', f'{end_date}#zzzzzzz')
        ),
        "ScanIndexForward": True
    }
    while True:
        response = _table(table_name).query(**kwargs)
        items.extend(response.get('Items', []))
        lek = response.get('LastEvaluatedKey')
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return items

def query_papers_by_keyword(table_name, keyword, limit=20):
    """
    Query 5: Papers containing keyword.
    Uses: GSI2 (KeywordIndex) partition key query.
    """
    response = _table(table_name).query(
        IndexName='KeywordIndex',
        KeyConditionExpression=Key('GSI2PK').eq(f'KW#{keyword.lower()}'),
        ScanIndexForward=False,
        Limit=limit
    )
    return response.get('Items', [])

def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("recent")
    p1.add_argument("category")
    p1.add_argument("--limit", type=int, default=20)
    p1.add_argument("--table", default=os.environ.get("DDB_TABLE"))

    p2 = sub.add_parser("author")
    p2.add_argument("author_name")
    p2.add_argument("--table", default=os.environ.get("DDB_TABLE"))

    p3 = sub.add_parser("get")
    p3.add_argument("arxiv_id")
    p3.add_argument("--table", default=os.environ.get("DDB_TABLE"))

    p4 = sub.add_parser("daterange")
    p4.add_argument("category")
    p4.add_argument("start_date")
    p4.add_argument("end_date")
    p4.add_argument("--table", default=os.environ.get("DDB_TABLE"))

    p5 = sub.add_parser("keyword")
    p5.add_argument("keyword")
    p5.add_argument("--limit", type=int, default=20)
    p5.add_argument("--table", default=os.environ.get("DDB_TABLE"))

    args = parser.parse_args()
    if not args.table:
        raise SystemExit("Missing table name. Use --table TABLE or set env DDB_TABLE.")

    if args.cmd == "recent":
        results, ms = _exec_timed(query_recent_in_category, args.table, args.category, args.limit)
        _json_out({
            "query_type": "recent_in_category",
            "parameters": {"category": args.category, "limit": args.limit},
            "results": results,
            "count": len(results),
            "execution_time_ms": ms
        })
    elif args.cmd == "author":
        results, ms = _exec_timed(query_papers_by_author, args.table, args.author_name)
        _json_out({
            "query_type": "papers_by_author",
            "parameters": {"author_name": args.author_name},
            "results": results,
            "count": len(results),
            "execution_time_ms": ms
        })
    elif args.cmd == "get":
        result, ms = _exec_timed(get_paper_by_id, args.table, args.arxiv_id)
        _json_out({
            "query_type": "get_paper_by_id",
            "parameters": {"arxiv_id": args.arxiv_id},
            "results": [result] if result else [],
            "count": 1 if result else 0,
            "execution_time_ms": ms
        })
    elif args.cmd == "daterange":
        results, ms = _exec_timed(query_papers_in_date_range, args.table, args.category, args.start_date, args.end_date)
        _json_out({
            "query_type": "papers_in_date_range",
            "parameters": {"category": args.category, "start_date": args.start_date, "end_date": args.end_date},
            "results": results,
            "count": len(results),
            "execution_time_ms": ms
        })
    elif args.cmd == "keyword":
        results, ms = _exec_timed(query_papers_by_keyword, args.table, args.keyword, args.limit)
        _json_out({
            "query_type": "papers_by_keyword",
            "parameters": {"keyword": args.keyword, "limit": args.limit},
            "results": results,
            "count": len(results),
            "execution_time_ms": ms
        })

if __name__ == "__main__":
    main()
