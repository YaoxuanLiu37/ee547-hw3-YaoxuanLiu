import argparse
import boto3
import botocore
import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from time import sleep

STOPWORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
    'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
    'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
    'can', 'this', 'that', 'these', 'those', 'we', 'our', 'use', 'using',
    'based', 'approach', 'method', 'paper', 'propose', 'proposed', 'show'
}

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("papers_json_path")
    p.add_argument("table_name")
    p.add_argument("--region", default=None)
    return p.parse_args()

def get_client_resource(region):
    if region:
        return boto3.client("dynamodb", region_name=region), boto3.resource("dynamodb", region_name=region)
    return boto3.client("dynamodb"), boto3.resource("dynamodb")

def ensure_table(client, resource, table_name):
    try:
        client.describe_table(TableName=table_name)
        return resource.Table(table_name)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    params = {
        "TableName": table_name,
        "AttributeDefinitions": [
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
            {"AttributeName": "GSI1SK", "AttributeType": "S"},
            {"AttributeName": "GSI2PK", "AttributeType": "S"},
            {"AttributeName": "GSI2SK", "AttributeType": "S"},
            {"AttributeName": "GSI3PK", "AttributeType": "S"},
            {"AttributeName": "GSI3SK", "AttributeType": "S"},
        ],
        "KeySchema": [
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "AuthorIndex",
                "KeySchema": [
                    {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["arxiv_id","title","categories","published"]},
            },
            {
                "IndexName": "KeywordIndex",
                "KeySchema": [
                    {"AttributeName": "GSI2PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI2SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["arxiv_id","title","categories","published"]},
            },
            {
                "IndexName": "PaperIdIndex",
                "KeySchema": [
                    {"AttributeName": "GSI3PK", "KeyType": "HASH"},
                    {"AttributeName": "GSI3SK", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
    }
    print(f"Creating DynamoDB table: {table_name}")
    client.create_table(**params)
    waiter = client.get_waiter('table_exists')
    waiter.wait(TableName=table_name)
    return resource.Table(table_name)

def load_papers(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def iso_to_date_str(iso_ts):
    return iso_ts[:10]

def tokenize(text):
    tokens = re.findall(r"[a-zA-Z]+", (text or "").lower())
    return [t for t in tokens if t not in STOPWORDS and len(t) > 2]

def top_k_keywords_from_abstract(abstract, k=10):
    cnt = Counter(tokenize(abstract))
    return [w for w, _ in cnt.most_common(k)]

def put_batch(table, items):
    with table.batch_writer(overwrite_by_pkeys=['PK','SK']) as batch:
        for it in items:
            batch.put_item(Item=it)

def main():
    args = parse_args()
    client, resource = get_client_resource(args.region)
    table = ensure_table(client, resource, args.table_name)

    print("Creating GSIs: AuthorIndex, PaperIdIndex, KeywordIndex")
    print(f"Loading papers from {args.papers_json_path}...")
    papers = load_papers(args.papers_json_path)

    total_papers = 0
    counts = defaultdict(int)
    all_items = []

    for p in papers:
        total_papers += 1
        arxiv_id = p.get("arxiv_id")
        title = p.get("title")
        authors = p.get("authors") or []
        abstract = p.get("abstract") or ""
        categories = p.get("categories") or []
        published_iso = p.get("published")
        published_date = iso_to_date_str(published_iso) if published_iso else "0000-00-00"
        keywords = top_k_keywords_from_abstract(abstract, k=10)

        detail_item = {
            "PK": f"PAPER#{arxiv_id}",
            "SK": "DETAIL",
            "GSI3PK": f"PAPER#{arxiv_id}",
            "GSI3SK": "DETAIL",
            "arxiv_id": arxiv_id,
            "title": title,
            "authors": authors,
            "abstract": abstract,
            "categories": categories,
            "keywords": keywords,
            "published": published_iso,
            "item_type": "PAPER_DETAIL",
        }
        all_items.append(detail_item)
        counts["paper_detail_items"] += 1

        for cat in categories:
            cat_item = {
                "PK": f"CATEGORY#{cat}",
                "SK": f"{published_date}#{arxiv_id}",
                "arxiv_id": arxiv_id,
                "title": title,
                "authors": authors,
                "abstract": abstract,
                "categories": categories,
                "keywords": keywords,
                "published": published_iso,
                "item_type": "CATEGORY_ITEM",
            }
            all_items.append(cat_item)
            counts["category_items"] += 1

        for au in authors:
            author_item = {
                "PK": f"AUTHORITEM#{au}",
                "SK": f"{published_date}#{arxiv_id}",
                "GSI1PK": f"AUTHOR#{au}",
                "GSI1SK": f"{published_date}#{arxiv_id}",
                "arxiv_id": arxiv_id,
                "title": title,
                "categories": categories,
                "published": published_iso,
                "item_type": "AUTHOR_ITEM",
            }
            all_items.append(author_item)
            counts["author_items"] += 1

        seen_kw = set()
        for kw in keywords:
            if kw in seen_kw:
                continue
            seen_kw.add(kw)
            kw_item = {
                "PK": f"KEYWORDITEM#{kw}",
                "SK": f"{published_date}#{arxiv_id}",
                "GSI2PK": f"KW#{kw}",
                "GSI2SK": f"{published_date}#{arxiv_id}",
                "arxiv_id": arxiv_id,
                "title": title,
                "categories": categories,
                "published": published_iso,
                "item_type": "KEYWORD_ITEM",
            }
            all_items.append(kw_item)
            counts["keyword_items"] += 1

    print("Extracting keywords from abstracts...")
    for i in range(0, len(all_items), 25):
        put_batch(table, all_items[i:i+25])

    total_items = sum(counts.values())
    denorm_factor = (total_items / total_papers) if total_papers else 0.0

    print(f"Loaded {total_papers} papers")
    print(f"Created {total_items} DynamoDB items (denormalized)")
    print(f"Denormalization factor: {denorm_factor:.1f}x")
    print("Storage breakdown:")
    print(f"  - Category items: {counts['category_items']}")
    print(f"  - Author items: {counts['author_items']}")
    print(f"  - Keyword items: {counts['keyword_items']}")
    print(f"  - Paper ID items: {counts['paper_detail_items']}")

if __name__ == "__main__":
    main()
