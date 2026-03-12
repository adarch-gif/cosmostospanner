# 04 - v2 Multi-API Router Quickstart

This guide runs the v2 pipeline end-to-end.

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

## 8. Optional job-scoped execution

```powershell
python .\scripts\v2_route_migrate.py --config .\config\v2.multiapi-routing.yaml --job mongo_users
```

## 9. Expected outputs/state

1. `runtime.state_file` (watermarks)
2. `runtime.route_registry_file` (destination map)
3. `runtime.dlq_file_path` (rejected/failed records when skip mode enabled)

