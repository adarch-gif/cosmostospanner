# 13 - Detailed Data Flow Diagram

This diagram focuses on runtime correctness, not every implementation detail.

It shows the five decisions that matter operationally:

1. release gating
2. distributed shard ownership
3. safe source resume
4. write / route / cleanup behavior
5. checkpoint advancement rules

```mermaid
flowchart TD
    Start["Start run"] --> Gate{"Prod release gate enabled?"}
    Gate -- Yes --> GateCheck["Validate fresh stage rehearsal"]
    Gate -- No --> Claim
    GateCheck --> Claim["Claim shard / acquire lease if distributed"]

    Claim --> Resume["Load reader cursor + committed checkpoint"]
    Resume --> Read["Read source page / record"]
    Read --> Transform["Transform document or canonicalize record"]

    Transform --> Pipeline{"Pipeline?"}

    Pipeline -- v1 --> V1Write["Write Spanner batch<br/>upsert or delete"]
    V1Write --> V1Ok{"Batch succeeded?"}
    V1Ok -- Yes --> SafeCursor["Persist last safe reader cursor"]
    V1Ok -- No, skip mode --> V1DLQ["Emit dead-letter entry"]
    V1Ok -- No, fail mode --> Fail["Fail shard / run"]

    Pipeline -- v2 --> Route["Route by payload size + overhead"]
    Route --> RouteType{"Destination?"}
    RouteType -- Firestore --> FireWrite["Upsert Firestore"]
    RouteType -- Spanner --> SpanWrite["Upsert Spanner"]
    RouteType -- Reject --> V2DLQ["Emit rejected record to DLQ"]

    FireWrite --> RouteMove{"Destination changed?"}
    SpanWrite --> RouteMove
    RouteMove -- No --> RouteComplete["Upsert complete route-registry entry"]
    RouteMove -- Yes --> Pending["Write pending_cleanup registry entry"]
    Pending --> Cleanup["Delete old sink copy"]
    Cleanup --> CleanupOk{"Cleanup succeeded?"}
    CleanupOk -- Yes --> RouteComplete
    CleanupOk -- No, skip mode --> CleanupDeferred["Keep pending_cleanup for retry"]
    CleanupOk -- No, fail mode --> Fail

    RouteComplete --> SafeCursor

    SafeCursor --> Flush{"Flush boundary reached?"}
    V1DLQ --> Flush
    V2DLQ --> Flush
    CleanupDeferred --> Flush
    Flush -- Yes --> FlushState["Flush registry / metrics / local state"]
    Flush -- No --> More{"More source records?"}
    FlushState --> More

    More -- Yes --> Read
    More -- No --> Advance{"Were writes safe and ordering valid?"}

    Advance -- Yes --> Commit["Advance checkpoint"]
    Advance -- No --> Hold["Hold checkpoint for replay safety"]

    Commit --> Clear["Clear reader cursor for completed shard"]
    Hold --> Mark["Mark shard completed or failed"]
    Clear --> Mark
    Mark --> End["End run"]
    Fail --> End
```

## Notes

1. Reader cursors are persisted only after a known-safe point, not for every source read.
2. Checkpoints advance only when the corresponding writes are confirmed safe.
3. In `v2`, route changes are durable before cleanup so reruns can reconcile interrupted moves.
4. In incremental `v2`, out-of-order records intentionally block checkpoint movement to avoid silent data loss.
