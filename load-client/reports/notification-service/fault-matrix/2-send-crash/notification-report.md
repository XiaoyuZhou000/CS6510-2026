# Notification report: fault-matrix/2-send-crash

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | fault-matrix/2-send-crash |
| startedAt | 2026-11-13T05:51:47.9386861+00:00 |
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
| sales.completed | 3753 |
| sales.activeWindowSeconds | 60.03 |
| sales.perSecond | 62.519 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 35184 |
| notifications.delivered | 35184 |
| notifications.received | 35184 |
| notifications.redeliveries | 12 |
| notifications.deliveredPerSecond | 585.374 |
| notifications.receivedPerSecond | 585.641 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 31 | 8479 | 10412 | 11567 | 35184 |
| toDelivered | 55 | 8485 | 10435 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 6 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 153 | 0 |
| channelLag | 134 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T05:55:36.998Z |
| allReceivedAt | 2026-11-13T05:55:37.032Z |
| drainSeconds | 0.034 |
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
