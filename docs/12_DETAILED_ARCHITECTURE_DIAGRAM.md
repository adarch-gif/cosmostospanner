# 12 - Detailed Architecture Diagram

This diagram is the authoritative high-level architecture view for the repository as it exists today.

It keeps the layout simple by separating the system into four layers:

1. source systems
2. execution entry points
3. runtime pipelines
4. state and target systems

```mermaid
flowchart LR
    subgraph Sources["1. Source Systems"]
        SQL["Cosmos SQL API"]
        Mongo["Cosmos Mongo API"]
        Cassandra["Cosmos Cassandra API"]
    end

    subgraph Entry["2. Execution Entry Points"]
        V1Pre["v1 preflight"]
        V1Run["v1 backfill / incremental"]
        V1Val["v1 validation"]
        V2Pre["v2 preflight"]
        V2Run["v2 routed migration"]
        V2Val["v2 routed validation"]
        Release["release gate"]
        Status["control-plane status"]
    end

    subgraph Pipelines["3. Runtime Pipelines"]
        direction TB

        subgraph V1["v1: Cosmos SQL API -> Spanner"]
            direction LR
            V1Cfg["config loader"]
            V1Read["CosmosReader"]
            V1Map["transform_document"]
            V1Write["SpannerWriter"]
            V1Err["DeadLetterSink"]
            V1Obs["structured logs + metrics"]
        end

        subgraph V2["v2: Mongo/Cassandra -> Routed Sinks"]
            direction LR
            V2Cfg["config loader"]
            V2Read["source adapters"]
            V2Route["SizeRouter"]
            V2Fire["Firestore sink"]
            V2Span["Spanner sink"]
            V2Reg["RouteRegistryStore"]
            V2Err["DeadLetterSink"]
            V2Obs["structured logs + metrics"]
            V2Recon["V2 reconciliation"]
        end
    end

    subgraph State["4. Shared State / Control Plane"]
        direction TB
        StateKinds["watermarks<br/>reader cursors<br/>leases<br/>progress manifests<br/>route registry"]
        Backends["local JSON<br/>GCS objects<br/>Spanner control-plane table"]
    end

    subgraph Targets["5. Targets / Outputs"]
        direction TB
        Spanner["Cloud Spanner"]
        Firestore["Firestore"]
        DLQ["JSONL dead-letter output"]
        Metrics["Prometheus / JSON metrics files"]
    end

    subgraph Infra["6. Provisioning / Safety"]
        Terraform["Terraform stacks"]
        Stage["stage rehearsal attestation"]
    end

    SQL --> V1Read
    Mongo --> V2Read
    Cassandra --> V2Read

    V1Pre --> V1Cfg
    V1Run --> V1Cfg
    V1Val --> V1Cfg

    V2Pre --> V2Cfg
    V2Run --> V2Cfg
    V2Val --> V2Cfg

    Release --> Stage
    Status --> StateKinds

    V1Cfg --> V1Read --> V1Map --> V1Write --> Spanner
    V1Map -->|transform or delete errors| V1Err --> DLQ
    V1Write -->|row fallback failures| V1Err
    V1Run --> V1Obs --> Metrics

    V2Cfg --> V2Read --> V2Route
    V2Route -->|small payload| V2Fire --> Firestore
    V2Route -->|large payload within safe cap| V2Span --> Spanner
    V2Route -->|oversized / rejected| V2Err --> DLQ
    V2Fire --> V2Reg
    V2Span --> V2Reg
    V2Run --> V2Obs --> Metrics
    V2Val --> V2Recon
    V2Recon --> V2Reg
    V2Recon --> Firestore
    V2Recon --> Spanner

    V1Run --> StateKinds
    V2Run --> StateKinds
    StateKinds --> Backends

    Terraform --> Spanner
    Terraform --> Firestore
    Terraform --> Backends
```

## Notes

1. `v1` and `v2` are intentionally separate execution paths. `v1` does not use the size router or Firestore.
2. `v1` incremental checkpoints are keyed by mapping and shard, not only by source container.
3. `v2` route registry entries are richer than simple ID mapping; they capture destination, checksum, payload size, and cleanup state.
4. Shared state can live locally, in GCS, or in a Spanner control-plane table depending on deployment maturity.
5. Distributed execution is coordinated through leases, progress manifests, and reader cursors rather than through in-memory worker state.
