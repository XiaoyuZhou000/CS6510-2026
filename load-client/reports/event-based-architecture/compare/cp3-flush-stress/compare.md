| Label | Workload | Variants | Pairs | A median (range) | B median (range) | Delta | B ahead | Classification | p99 regression |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| `cp3-flush-stress` | stress | A: KAFKA_FLUSH_EVERY_MESSAGE=false; B: KAFKA_FLUSH_EVERY_MESSAGE=true | 1 | 10936 (10936-10936) | 9179 (9179-9179) | -16.07% | 0 of 1 | regression | scanP99Ms +14.1%, completeP99Ms +33.0% |
