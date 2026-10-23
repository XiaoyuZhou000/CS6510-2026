| Label | Workload | Variants | Pairs | A median (range) | B median (range) | U | Decision | Deciding metric |
|---|---|---|---:|---:|---:|---:|---|---|
| `cp3-inv10-32-stress` | stress | A: defaults; B: INVENTORY_DB_POOL_MAX_SIZE=32, INVENTORY_DB_POOL_SIZE=10 | 5 | 11000 (10486-11183) | 10945 (10920-11280) | 13 | B (tie-break) | inventoryLockWaitP99Ms |
