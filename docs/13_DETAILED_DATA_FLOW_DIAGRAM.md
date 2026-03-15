# 13 - Detailed Data Flow Diagram

This sequence diagram captures the runtime behavior that matters most for correctness:

1. release gating
2. distributed lease / shard handling
3. restart-safe reader cursors
4. success-aware checkpoint advancement
5. v2 route-registry move semantics

```mermaid
sequenceDiagram
    autonumber
    actor Scheduler as Operator / Scheduler
    participant Runner as Migration Runner
    participant Gate as Release Gate
    participant Lease as Lease / Progress Store
    participant Cursor as Reader Cursor Store
    participant Checkpoint as Watermark Store
    participant Source as Cosmos Source API
    participant Transform as Transform / Canonicalize
    participant Router as SizeRouter
    participant Sink as Spanner / Firestore Sink
    participant Registry as Route Registry
    participant DLQ as Dead Letter Sink
    participant Obs as Logs / Metrics

    Scheduler->>Runner: start run(config, mode, filters)
    opt prod gating enabled
        Runner->>Gate: validate fresh stage rehearsal
        Gate-->>Runner: allowed or blocked
    end

    opt distributed execution
        Runner->>Lease: acquire lease / claim shard work item
        Lease-->>Runner: granted or skip
    end

    Runner->>Cursor: load last safe reader position
    Cursor-->>Runner: resume token / last emitted key
    Runner->>Checkpoint: load last committed checkpoint
    Checkpoint-->>Runner: watermark or watermark + route-key boundary
    Note over Runner,Checkpoint: v1 uses mapping+shard checkpoints. v2 uses inclusive replay with route-key dedup at watermark boundaries.

    loop each source page or canonical record
        Runner->>Source: read next page / record
        Source-->>Runner: document + watermark + cursor
        Runner->>Transform: map fields or build canonical record

        alt v1 pipeline
            Transform-->>Runner: row or delete operation
            Runner->>Sink: batch upsert/delete in Spanner
            alt batch success
                Sink-->>Runner: committed
                Runner->>Cursor: persist last safe source boundary
            else failure and error_mode=skip
                Sink-->>Runner: write error
                Runner->>DLQ: emit failed row or document
                Runner->>Obs: increment failure metrics
            end

        else v2 pipeline
            Transform-->>Runner: canonical record
            Runner->>Router: decide by payload bytes + overhead

            alt route = Firestore
                Router-->>Runner: FIRESTORE
                Runner->>Sink: upsert Firestore record
            else route = Spanner
                Router-->>Runner: SPANNER
                Runner->>Sink: upsert Spanner record
            else route = reject
                Router-->>Runner: REJECT
                Runner->>DLQ: emit rejected record
                Runner->>Obs: increment rejected metrics
            end

            alt destination changed
                Runner->>Registry: write pending_cleanup entry
                Runner->>Sink: delete old sink copy
                Sink-->>Runner: cleanup success or failure
            end

            opt successful routed write
                Runner->>Registry: upsert complete route entry
                Runner->>Cursor: persist last safe source boundary
            end
        end

        opt batch flush boundary
            Runner->>Registry: flush registry state (v2)
            Runner->>Obs: flush metrics snapshot
        end
    end

    alt no record failures and ordering is safe
        Runner->>Checkpoint: advance committed checkpoint
        Runner->>Cursor: clear cursor for completed shard
    else failures or out-of-order incremental records
        Runner->>Obs: log checkpoint blocked
        Note over Runner,Checkpoint: Checkpoint is intentionally held back to preserve replay safety.
    end

    opt distributed full run
        Runner->>Lease: mark shard completed or failed
    end
```

## Notes

1. Reader cursors are persisted only after a known-safe point, not for every source read.
2. Checkpoints advance only when the corresponding writes are confirmed safe.
3. In `v2`, route changes are durable before cleanup so reruns can reconcile interrupted moves.
4. In incremental `v2`, out-of-order records intentionally block checkpoint movement to avoid silent data loss.
