# SQLite DB Implementation (Simplified Version)

## Features
 - Supports queries about database meta info (`.dbinfo` and `.tables`)
 - Supports simple `SELECT` queries
 - Supports specifying condition in `WHERE` clause
 - Supports `COUNT(*)` aggregation query
 - Supports using `index` to find rows efficiently 

### Sample Queries
```
SELECT COUNT(*) from table_1
```

```
SELECT col1, col2 FROM table_1
```

```
SELECT col1, col2 FROM table_1 WHERE col3="value"
```
