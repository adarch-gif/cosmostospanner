# 12 - Detailed Architecture Diagram

This diagram is the authoritative high-level architecture view for the repository as it exists today.

It separates:

1. `v1` SQL API -> Spanner migration
2. `v2` Mongo/Cassandra -> Firestore/Spanner routed migration
3. shared control-plane backends for distributed execution and restart safety
4. provisioning and release-gate support

```mermaid
flowchart TB
    subgraph Azure["Azure Source Environment"]
        SQL["Cosmos DB<br/>SQL API"]
        Mongo["Cosmos DB<br/>Mongo API"]
        Cassandra["Cosmos DB<br/>Cassandra API"]
    end

    subgraph Entry["Execution / Entry Points"]
        V1Pre["scripts/preflight.py"]
        V1Run["scripts/backfill.py"]
        V1Val["scripts/validate.py"]
        V2Pre["scripts/v2_preflight.py"]
        V2Run["scripts/v2_route_migrate.py"]
        V2Val["scripts/v2_validate.py"]
        Release["scripts/release_gate.py"]
        Status["scripts/control_plane_status.py"]
    end

    subgraph V1["v1 Pipeline: Cosmos SQL API -> Spanner"]
        V1Cfg["load_config()<br/>migration/config.py"]
        V1Reader["CosmosReader"]
        V1Transform["transform_document()"]
        V1Writer["SpannerWriter"]
        V1DLQ["DeadLetterSink"]
        V1WM["WatermarkStore<br/>(mapping + shard scoped)"]
        V1Cursor["ReaderCursorStore"]
        V1Coord["WorkCoordinator<br/>lease + progress"]
        V1Obs["Structured logs<br/>MetricsCollector"]
    end

    subgraph V2["v2 Pipeline: Mongo/Cassandra -> Routed Sinks"]
        V2Cfg["load_v2_config()<br/>migration_v2/config.py"]
        MongoAdapter["MongoCosmosSourceAdapter"]
        CassAdapter["CassandraCosmosSourceAdapter"]
        Router["SizeRouter<br/>payload bytes + overhead"]
        FireSink["FirestoreSinkAdapter"]
        SpanSink["SpannerSinkAdapter"]
        Registry["RouteRegistryStore<br/>destination + checksum + sync_state"]
        V2WM["WatermarkStore<br/>(inclusive replay checkpoint)"]
        V2Cursor["ReaderCursorStore"]
        V2Coord["WorkCoordinator<br/>lease + progress"]
        V2Recon["V2ReconciliationRunner"]
        V2DLQ["DeadLetterSink"]
        V2Obs["Structured logs<br/>MetricsCollector"]
    end

    subgraph Control["State / Control Plane Backends"]
        Local["Local JSON files"]
        GCS["GCS objects"]
        CPSpanner["Spanner control-plane table"]
    end

    subgraph GCP["GCP Targets / Outputs"]
        Spanner["Cloud Spanner"]
        Firestore["Firestore"]
        DLQFile["JSONL DLQ files"]
        MetricsOut["Prometheus / JSON metrics snapshots"]
    end

    subgraph Infra["Provisioning / Safety Controls"]
        TF["Terraform stacks + env wrappers"]
        Gate["Stage rehearsal attestation"]
    end

    SQL --> V1Reader
    Mongo --> MongoAdapter
    Cassandra --> CassAdapter

    V1Pre --> V1Cfg
    V1Run --> V1Cfg
    V1Val --> V1Cfg

    V2Pre --> V2Cfg
    V2Run --> V2Cfg
    V2Val --> V2Cfg

    Release --> Gate
    Status --> V1Coord
    Status --> V2Coord

    V1Cfg --> V1Reader
    V1Reader --> V1Transform
    V1Transform --> V1Writer
    V1Writer --> Spanner
    V1Transform -->|transform / delete failures| V1DLQ
    V1Writer -->|row fallback failures| V1DLQ
    V1DLQ --> DLQFile
    V1Cfg --> V1WM
    V1Cfg --> V1Cursor
    V1Cfg --> V1Coord
    V1Run --> V1Obs --> MetricsOut

    V2Cfg --> MongoAdapter
    V2Cfg --> CassAdapter
    MongoAdapter --> Router
    CassAdapter --> Router
    Router -->|payload < threshold| FireSink
    Router -->|threshold <= payload <= safe cap| SpanSink
    Router -->|oversized / rejected| V2DLQ
    FireSink --> Firestore
    SpanSink --> Spanner
    V2DLQ --> DLQFile
    FireSink --> Registry
    SpanSink --> Registry
    V2Cfg --> V2WM
    V2Cfg --> V2Cursor
    V2Cfg --> V2Coord
    V2Run --> V2Obs --> MetricsOut
    V2Val --> V2Recon
    V2Recon --> Registry
    V2Recon --> Firestore
    V2Recon --> Spanner

    V1WM --- Local
    V1WM --- GCS
    V1WM --- CPSpanner
    V1Cursor --- Local
    V1Cursor --- GCS
    V1Cursor --- CPSpanner
    V1Coord --- Local
    V1Coord --- GCS
    V1Coord --- CPSpanner

    Registry --- Local
    Registry --- GCS
    Registry --- CPSpanner
    V2WM --- Local
    V2WM --- GCS
    V2WM --- CPSpanner
    V2Cursor --- Local
    V2Cursor --- GCS
    V2Cursor --- CPSpanner
    V2Coord --- Local
    V2Coord --- GCS
    V2Coord --- CPSpanner

    TF --> Spanner
    TF --> Firestore
    TF --> GCS
    TF --> CPSpanner
```

## Notes

1. `v1` and `v2` are intentionally separate execution paths. `v1` does not use the size router or Firestore.
2. `v1` incremental checkpoints are keyed by mapping and shard, not only by source container.
3. `v2` route registry entries are richer than simple ID mapping; they capture destination, checksum, payload size, and cleanup state.
4. Shared state can live locally, in GCS, or in a Spanner control-plane table depending on deployment maturity.
5. Distributed execution is coordinated through leases, progress manifests, and reader cursors rather than through in-memory worker state.
