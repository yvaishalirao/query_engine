# Query Engine

A command-line tool that runs SQL-like queries against CSV files. It parses the query through a formal grammar, builds a typed AST, and executes it as an explicit pandas pipeline.

---

## Installation

```bash
pip install -r requirements.txt
```

Requires Python 3.10+. Dependencies: `lark`, `pandas`, `tabulate`.

---

## Usage

**Single query:**
```bash
python main.py --csv data/sample.csv "SELECT region, SUM(sales) AS total FROM data GROUP BY region ORDER BY total DESC LIMIT 3"
```

**Interactive mode:**
```bash
python main.py --csv data/sample.csv
# Enter query (or 'quit' to exit): SELECT * FROM data LIMIT 5
```

`--csv` defaults to `data/sample.csv` if omitted.

---

## Supported Syntax

### SELECT

```sql
SELECT *
SELECT col1, col2
SELECT region, AVG(sales) AS avg_sales
SELECT COUNT(product) AS n, region
```

Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`. Each accepts a single column argument. Aliases via `AS`.

### FROM

```sql
FROM data
```

Table name is the CSV identifier — it doesn't affect which file is loaded (the file path is set via `--csv`).

### WHERE

```sql
WHERE year = 2023
WHERE sales >= 1000 AND region = 'North'
WHERE price != 9.99 AND quantity < 50
```

Supported operators: `=`, `!=`, `<`, `>`, `<=`, `>=`

String values: single or double quotes. Numeric values: unquoted integers or floats.

Multiple conditions must be joined with `AND`. See [Not Supported](#not-supported) for OR.

### GROUP BY

```sql
GROUP BY region
GROUP BY region, product
```

### ORDER BY

```sql
ORDER BY sales DESC
ORDER BY region ASC
```

Direction defaults to `ASC` if omitted. Single column only.

### LIMIT

```sql
LIMIT 10
```

Applied after `ORDER BY`, so you always get the correct top-N.

---

## Full Example

```bash
python main.py --csv data/sample.csv \
  "SELECT region, AVG(sales) AS avg_s FROM data WHERE year = 2023 GROUP BY region ORDER BY avg_s DESC LIMIT 3"
```

Output:
```
region      avg_s
--------  -------
North        4350
West         3150
East         2633
```

---

## Not Supported

These are rejected at parse time — you'll get an `Error:` message, not a partial result.

| Construct | Example |
|-----------|---------|
| OR conditions | `WHERE a > 1 OR b < 2` |
| JOIN | `FROM a JOIN b ON ...` |
| Subqueries | `FROM (SELECT ...)` |
| HAVING | `GROUP BY x HAVING COUNT(*) > 1` |
| INSERT / UPDATE / DELETE | any write operation |

---

## Known Differences from Standard SQL

**NULL handling:** Missing values follow pandas NaN semantics. Comparisons against NaN propagate NaN (not `FALSE` as SQL requires). If your CSV has blank cells, filter results may differ from what a SQL engine would return.

**Type coercion:** Column types are inferred by pandas on CSV load. A column of `"1"`, `"2"`, `"3"` becomes int64; mixed types become object. Arithmetic aggregates (`SUM`, `AVG`) on an object column raise an error rather than coercing silently.

**Case sensitivity:** Column names and string values in WHERE are case-sensitive. Keywords (`SELECT`, `FROM`, etc.) are case-insensitive.

---

## Error Messages

All errors are prefixed with `Error:` — no stack traces reach the terminal.

| Message | Cause |
|---------|-------|
| `Error: CSV file not found: path` | The file passed to `--csv` does not exist |
| `Error: Column not found: col` | A column in SELECT, WHERE, or GROUP BY isn't in the CSV |
| `Error: Aggregation AVG requires numeric column: col` | SUM/AVG/MIN/MAX applied to a string column |
| `Error: Syntax error in query near: ...` | Unsupported syntax (OR, subquery, etc.) or malformed query |
