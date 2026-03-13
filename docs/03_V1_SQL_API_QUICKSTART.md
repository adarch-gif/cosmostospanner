# 03 - v1 SQL API Quickstart

This guide runs the v1 pipeline end-to-end.

## 1. Install dependencies

```powershell
cd C:\Users\ados1\cosmos-to-spanner-migration
python -m pip install -r requirements.txt
```

## 2. Prepare v1 config

```powershell
Copy-Item .\config\migration.example.yaml .\config\migration.yaml
```

Fill values in `config/migration.yaml`.

For multi-runner or shared-runner operation, prefer a `gs://bucket/object.json` value for `runtime.watermark_state_file`.

## 3. Set required secrets/environment

```powershell
$env:COSMOS_KEY = "<cosmos-sql-key>"
$env:GOOGLE_CLOUD_PROJECT = "<gcp-project-id>"
# Optional if not already configured via ADC
$env:GOOGLE_APPLICATION_CREDENTIALS = "C:\path\to\gcp-sa.json"
```

## 4. Run v1 preflight

```powershell
python .\scripts\preflight.py --config .\config\migration.yaml
```

## 5. Run dry-run

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --dry-run
```

## 6. Run full backfill

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml
```

## 7. Validate

```powershell
python .\scripts\validate.py --config .\config\migration.yaml --sample-size 200
```

For production cutover validation, use full checksum reconciliation:

```powershell
python .\scripts\validate.py --config .\config\migration.yaml --reconciliation-mode checksums
```

## 8. Incremental mode

```powershell
python .\scripts\backfill.py --config .\config\migration.yaml --incremental
```

## 9. Common success criteria

1. Preflight exits 0
2. No critical errors in backfill logs
3. Validation returns 0
4. DLQ count is zero or investigated
5. Checksum reconciliation passes before cutover when using high-assurance migrations
