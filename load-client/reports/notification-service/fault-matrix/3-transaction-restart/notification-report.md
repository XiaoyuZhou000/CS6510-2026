# Notification report: fault-matrix/3-transaction-restart

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | fault-matrix/3-transaction-restart |
| startedAt | 2026-11-13T05:55:58.3590901+00:00 |
| deploymentCommit | bc85a457228d41b0982cb055686942f14c4018c9 |
| subscribers | 100 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":10,"durationSeconds":60} |
| faults | none |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 2985 |
| sales.activeWindowSeconds | 60.04 |
| sales.perSecond | 49.717 |
| sales.backgroundCompletions | 10 |
| notifications.owed | 28199 |
| notifications.delivered | 28199 |
| notifications.received | 28199 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 469.387 |
| notifications.receivedPerSecond | 469.437 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 26 | 89 | 172 | 2879 | 28199 |
| toDelivered | 47 | 118 | 206 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 5 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 51 | 0 |
| channelLag | 88 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T05:58:47.063Z |
| allReceivedAt | 2026-11-13T05:58:47.094Z |
| drainSeconds | 0.031 |
| deadlineSeconds | 60 |
| deadlineMet | true |

## Reconciliation counts

| Category | Count |
|---|---|
| saleEvents | 0 |
| fixedSubscriptions | 0 |
| history | 0 |
| selectionMismatch | 0 |
| missingNotDelivered | 0 |
| missingDeliveredNotReceived | 0 |
| unexpected | 0 |
| duplicate | 0 |
| quantityMismatch | 0 |
| parked | 0 |
