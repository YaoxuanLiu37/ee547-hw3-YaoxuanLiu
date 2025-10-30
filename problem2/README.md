# Problem 2 – DynamoDB Schema, API, and EC2 Deployment Analysis

## 1. Schema Design Decisions

### Partition Key Structure
The main table uses:
- **Partition key (PK):** `CATEGORY#{category}`
- **Sort key (SK):** `{published_date}#{arxiv_id}`

This structure supports efficient access to recent papers within a given category, with items automatically sorted by publication date. Range queries by date (using `between`) are also efficient.

### Global Secondary Indexes (GSIs)
Three GSIs were created to support all required access patterns:

| GSI Name | Partition Key | Purpose |
|-----------|----------------|----------|
| **AuthorIndex** | `AUTHOR#{author_name}` | Query papers by author |
| **PaperIdIndex** | `PAPER#{arxiv_id}` | Retrieve full paper details by ID |
| **KeywordIndex** | `KW#{keyword}` | Query papers by keyword |

Each GSI directly maps to one query endpoint, allowing constant-time access without table scans.

### Denormalization Trade-offs
The dataset is denormalized to enable fast lookup:
- Each paper is written once per **category**, **author**, and **keyword**.
- This improves query latency (<100 ms) at the cost of higher storage.

**Trade-off summary:**
- **Pros:** All queries use key lookups or GSI queries, no scans.
- **Cons:** Storage grows ~15× and updates require multiple writes.
- Given the read-heavy and append-only nature of the dataset, this is acceptable.

---

## 2. Denormalization Analysis

From `load_data.py` execution:

```
Loaded 10 papers
Created 150 DynamoDB items (denormalized)
Denormalization factor: 15.0x
```

| Metric | Value | Explanation |
|---------|--------|-------------|
| Average DynamoDB items per paper | 15 | Each paper appears in multiple indexes |
| Storage multiplication factor | 15× | Total items / original papers |
| Most duplicated dimensions | Keywords and authors | Each keyword and author adds an item |

---

## 3. Query Limitations

While all required endpoints are efficient, several queries are not supported well in DynamoDB:

| Query Type | Limitation |
|-------------|-------------|
| “Count total papers by author” | No built-in aggregation; requires full table scan |
| “Most cited papers globally” | No global sorting or aggregation functions |
| “Fuzzy keyword search” | Only exact key lookups are supported; needs OpenSearch for full-text |
| “Multi-condition filters (keyword + date)” | DynamoDB cannot combine filters across GSIs |

**Reason:**  
DynamoDB is optimized for key-value access and fixed query patterns.  
It does not support `JOIN`, `GROUP BY`, or full-text search like relational databases.

---

## 4. When to Use DynamoDB

| Scenario | first choice DB | Reason |
|-----------|----------------|--------|
| Fixed, predictable access patterns | DynamoDB | Low latency, massive scalability |
| Dynamic ad-hoc queries | PostgreSQL | Full SQL, flexible filters and joins |
| Frequent updates or complex transactions | PostgreSQL | Easier consistency management |
| Read-heavy, append-only workloads | DynamoDB | High-speed reads, automatic scaling |
| Cost-sensitive serverless workloads | DynamoDB | Pay-per-request, zero maintenance |

**Trade-off Summary:**  
DynamoDB trades query flexibility for performance and scalability.  
PostgreSQL offers richer queries but higher operational overhead.

---

## 5. EC2 Deployment

| Item | Detail |
|------|--------|
| **Public IP** | `54.219.182.247` |
| **IAM Role ARN** | `arn:aws:iam::124878108354:user/ee547-problem3` |
| **OS** | Amazon Linux 2023 |
| **Deployment Challenges** | ① `.pem` permission issue during SSH; ② IAM AccessDenied; ③ missing region in boto3; ④ security group initially blocked port 8080. |
| **Final Status** | Server deployed successfully, all five endpoints returned valid JSON within 30–70 ms. |

---

## 6. Validation Summary

All endpoints were tested from local WSL using `curl`:

| Endpoint | Test URL | Result |
|-----------|-----------|---------|
| `/papers/recent` | `?category=cs.LG&limit=5` | Returned recent papers |
| `/papers/author/{author_name}` | `/author/Hendrik Blockeel` | Returned 3 papers |
| `/papers/{arxiv_id}` | `/0110036v1` | Returned full paper details |
| `/papers/search` | `?category=cs.LG&start=2000-01-01&end=2002-01-01` | Returned all papers in range |
| `/papers/keyword/{keyword}` | `/keyword/decision?limit=5` | Returned keyword matches |

Error handling (400/404) was also verified with invalid inputs.  
Average response time was < 100 ms for all queries.

---

## Conclusion

All required API endpoints, GSIs, and non-normalized structures were implemented and verified.  
The EC2 deployment is accessible publicly and meets the < 200 ms latency requirement for up to 500 papers.  
The design aligns with all specifications.
