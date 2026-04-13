# Query Engine

A command-line tool that runs SQL-like queries against CSV files. It parses the query through a formal grammar, builds a typed AST, and executes it as an explicit pandas pipeline. Supports full read queries (SELECT with joins, subqueries, aggregation) and write operations (INSERT, UPDATE, DELETE) with automatic backup before any mutation.

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
python main.py --csv data/sample.csv "SELECT region, SUM(sales) AS total FROM data GROUP BY region ORDER BY total DESC"
```

**Write operation:**
```bash
python main.py --csv data/sample.csv "INSERT INTO data (region, product, sales, quantity, year) VALUES ('North', 'Widget', 1500, 10, 2024)"
python main.py --csv data/sample.csv "UPDATE data SET sales = 9999 WHERE region = 'East'"
python main.py --csv data/sample.csv "DELETE FROM data WHERE year = 2020"
```

**Interactive mode:**
```bash
python main.py --csv data/sample.csv
# Enter query (or 'quit' to exit): SELECT * FROM data LIMIT 5
# Enter query (or 'quit' to exit): UPDATE data SET sales = 0 WHERE region = 'South'
```

`--csv` defaults to `data/sample.csv` if omitted. Write operations modify the CSV in place and create a `.bak` backup before writing.

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
FROM (SELECT region, sales FROM data WHERE year = 2023) AS sub
```

The table name is the CSV identifier — the actual file is set via `--csv`. Subqueries in FROM are supported to arbitrary nesting depth (max 3).

### JOIN

```sql
INNER JOIN products ON product = name
LEFT JOIN regions ON region = region_id
INNER JOIN products ON data.product = products.name
```

`INNER JOIN` and `LEFT JOIN` are supported. The ON clause must be a column equality (`col = col`). Table-qualified column names (`table.column`) are accepted in ON, SELECT, and WHERE. Multiple joins can be chained.

### WHERE

```sql
WHERE year = 2023
WHERE sales >= 1000 AND region = 'North'
WHERE sales > 5000 OR year = 2021
WHERE price != 9.99 AND quantity < 50
```

Operators: `=`, `!=`, `<`, `>`, `<=`, `>=`. Boolean: `AND` and `OR` with standard SQL precedence (AND binds tighter than OR). String values: single or double quotes. Numeric values: unquoted integers or floats.

### GROUP BY / HAVING

```sql
GROUP BY region
GROUP BY region, product
GROUP BY region HAVING SUM(sales) > 10000
```

HAVING accepts the same boolean expressions as WHERE, applied after aggregation. HAVING requires GROUP BY.

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

Applied after ORDER BY, so you always get the correct top-N.

---

## Write Operations

Write statements modify the CSV file directly. A `.bak` backup is created before every write. If the write fails for any reason, the original file is restored automatically.

### INSERT

```sql
INSERT INTO data (region, product, sales, quantity, year) VALUES ('West', 'Gadget', 2200, 15, 2024)
```

Appends one row. Column list and value list must have the same length. All named columns must exist in the CSV.

### UPDATE

```sql
UPDATE data SET sales = 9999 WHERE region = 'East'
UPDATE data SET sales = 0, quantity = 0 WHERE year = 2020
```

Updates matching rows in place. Supports multiple SET assignments separated by commas. WHERE clause is optional — omitting it updates all rows.

### DELETE

```sql
DELETE FROM data WHERE year = 2020
```

Removes matching rows. WHERE is required — `DELETE FROM data` without a WHERE clause is rejected to prevent accidental data loss.

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

Join query:
```bash
python main.py --csv data/sample.csv \
  "SELECT region, category, SUM(sales) AS total FROM data INNER JOIN products ON product = name GROUP BY region, category ORDER BY total DESC"
```

---

## Not Supported

These are rejected at parse time — you'll get an `Error:` message, not a partial result.

| Construct | Example |
|-----------|---------|
| RIGHT JOIN / FULL OUTER JOIN | `FROM a RIGHT JOIN b ON ...` |
| Non-equality JOIN conditions | `ON a.sales > b.min_sales` |
| Subqueries in SELECT or WHERE | `WHERE id IN (SELECT ...)` |
| Multi-column ORDER BY | `ORDER BY region, sales DESC` |

---

## Known Differences from Standard SQL

**NULL handling:** Missing values follow pandas NaN semantics. Comparisons against NaN propagate NaN (not `FALSE` as SQL requires). If your CSV has blank cells, filter results may differ from what a SQL engine would return.

**Type coercion:** Column types are inferred by pandas on CSV load. A column of `"1"`, `"2"`, `"3"` becomes int64; mixed types become object. Arithmetic aggregates (`SUM`, `AVG`) on an object column raise an error rather than coercing silently.

**Case sensitivity:** Column names and string values in WHERE are case-sensitive. Keywords (`SELECT`, `FROM`, `INSERT`, etc.) are case-insensitive.

**DELETE safety:** Unguarded `DELETE FROM table` (no WHERE) is rejected by the engine. This is stricter than standard SQL to prevent accidental table truncation.

---

## Error Messages

All errors are prefixed with `Error:` — no stack traces reach the terminal.

| Message | Cause |
|---------|-------|
| `Error: CSV file not found: path` | The file passed to `--csv` does not exist |
| `Error: Column not found: col` | A column in SELECT, WHERE, GROUP BY, or SET isn't in the CSV |
| `Error: Aggregation AVG requires numeric column: col` | SUM/AVG/MIN/MAX applied to a string column |
| `Error: Syntax error in query near: ...` | Unsupported syntax or malformed query |
| `Error: INSERT column/value count mismatch: N column(s) but M value(s)` | Column list and value list lengths differ |
| `Error: DELETE without WHERE is not allowed` | DELETE issued without a WHERE clause |
| `Error: Join table not found: path` | A JOIN references a CSV that doesn't exist in the same directory |
| `Error: Write failed, original file restored: reason` | I/O error during write — backup was used to restore the file |
| `Error: Subquery nesting depth exceeded (max 3)` | More than 3 levels of nested subqueries |
