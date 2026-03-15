# 04 - v2 Multi-API Router Quickstart

This guide runs the v2 pipeline end-to-end.

For the full routed-architecture, route-registry, cleanup, validation, and cutover model, read:

1. `docs/ARCHITECTURE.md`
2. `docs/RUNBOOK.md`
3. `docs/12_DETAILED_ARCHITECTURE_DIAGRAM.md`
4. `docs/13_DETAILED_DATA_FLOW_DIAGRAM.md`

## 1. Install dependencies

```powershell
cd C:\Users\ados1\cosmos-to-spanner-migration
python -m pip install -r requirements.txt
```

## 2. Prepare v2 config

```powershell
Copy-Item .\config\v2.multiapi-routing.example.yaml .\config\v2.multiapi-routing.yaml
```

Fill values in `config/v2.multiapi-routing.yaml`.

For shared runners, prefer a shared backend for `runtime.state_file` and `runtime.route_registry_file`, either `gs://bucket/object.json` or `spanner://<project>/<instance>/<database>/<table>?namespace=<name>`.

## 3. Set required environment variables

```powershell
$env:COSMOS_MONGO_CONNECTION_STRING = "<mongo-connection-string>"
$env:COSMOS_CASSANDRA_USERNAME = "<cassandra-username>"
$env:COSMOS_CASSANDRA_PASSWORD = "<cassandra-password>"
$env:GOOGLE_CLOUD_PROJECT = "<gcp-project-id>"
$env:GOOGLE_APPLICATION_CREDENTIALS = "C:\path\to\gcp-sa.json"
```

## 4. Run v2 preflight

```powershell
python .\scripts\v2_preflight.py --config .\config\v2.multiapi-routing.yaml
```

## 5. Run dry-run

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --dry-run
```

## 6. Run full migration

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml
```

## 7. Run incremental

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --incremental
```

Incremental v2 runs now replay inclusively from the last watermark and deduplicate by `route_key` at the watermark boundary. This is intentional and reduces missed-record risk.

## 8. Run exact validation

```powershell
python .\scripts\v2_validate.py --config .\config\v2.multiapi-routing.yaml
```

This validation pass compares the selected source jobs against both the route registry and the union of Firestore + Spanner. It is exact, disk-backed, and suitable for cutover validation on large datasets.

## 9. Optional job-scoped execution

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --job mongo_users
```

## 10. Expected outputs/state

1. `runtime.state_file` (watermarks)
2. `runtime.route_registry_file` (destination map)
3. `runtime.dlq_file_path` (rejected/failed records when skip mode enabled)
4. Default Spanner routing ceiling is `8 MiB`; records above that are rejected or DLQ'd based on `error_mode`
