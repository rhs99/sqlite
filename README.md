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

### Code Execution
Run the script called `runner.sh`.
\
It will run different queries on the `sample.db` (already present in the repository).

### Using Other DB

There is another script called `download_sample_databases.sh`.
\
Executing this script will download two more database (`superheroes.db` and `companies.db`).

You can change the `DB_PATH` variable on the first line of `runner.sh` script and the queries accordingly to run on different database.


### Using Index

In the `companies.db` there is an index on `country` column. So the following query will use that index and the execution time will improve.

```
 SELECT name, country FROM companies WHERE country = 'chad'
```