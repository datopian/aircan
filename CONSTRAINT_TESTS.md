# Aircan Constraint Matrix

This is an end-to-end test project for the Aircan Table Schema validation path.
It creates CKAN resources, supplies a Frictionless descriptor through the CKAN
resource's singular `schema` field, waits for the
`pipeline_ckan_to_bigquery` Airflow DAG, and writes a detailed Markdown report.

## Run

From this directory:

```bash
uv sync
CKAN_API_TOKEN_FILE=/path/to/local/token uv run python constraint_matrix.py
```

Defaults target the local stack:

- CKAN: `http://localhost:5000`
- Datastore: `http://localhost:5000`
- Airflow: `http://localhost:8082`
- Airflow credentials: `airflow` / `airflow`
- Report: `reports/aircan-constraint-matrix.md`

Override endpoints or credentials with CLI options or environment variables:

```bash
uv run python constraint_matrix.py \
  --ckan-token-file /path/to/token \
  --report reports/my-run.md
```

## Cases

The matrix contains 21 cases:

1. Valid baseline
2. Required
3. String minimum length
4. String maximum length
5. Pattern
6. String enum
7. Number minimum
8. Number maximum
9. Number enum
10. Integer type
11. Number type
12. Boolean type
13. Date minimum
14. Date maximum
15. Date format
16. Datetime format
17. Email format
18. URI format
19. UUID format
20. Unique values
21. Multiple constraints in one row

The console output gives a short explanation for every case. The report also
contains the submitted CSV, schema, expected behavior, actual behavior, Airflow
task states, datastore output for the valid case, and failed-task log excerpts
when Airflow exposes them.
