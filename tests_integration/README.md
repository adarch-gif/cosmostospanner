# Integration Tests

These tests exercise the repository against live Azure Cosmos DB and GCP targets.

They are intentionally skipped unless you opt in.

## Enable

Set:

1. `RUN_LIVE_INTEGRATION_TESTS=true`
2. `COSMOS_TO_SPANNER_V1_INTEGRATION_CONFIG=<absolute path to live v1 config>`
3. `COSMOS_TO_SPANNER_V2_INTEGRATION_CONFIG=<absolute path to live v2 config>`

Then run:

```powershell
python -m pytest -q -m integration
```

## Expectations

1. The config files must reference real reachable cloud resources.
2. Required secrets and ADC credentials must already be present in the environment.
3. These tests perform connectivity and schema/source-access checks only.
4. Run them before production cutover and after Terraform changes.
