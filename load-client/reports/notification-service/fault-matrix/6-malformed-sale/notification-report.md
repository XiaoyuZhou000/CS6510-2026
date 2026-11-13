# Notification report: fault-matrix/6-malformed-sale

Reconciliation: **FAIL**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | fault-matrix/6-malformed-sale |
| startedAt | 2026-11-13T05:46:06.6074564+00:00 |
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
| sales.completed | 3755 |
| sales.activeWindowSeconds | 59.993 |
| sales.perSecond | 62.591 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 35439 |
| notifications.delivered | 35439 |
| notifications.received | 35439 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 590.002 |
| notifications.receivedPerSecond | 590.602 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 27 | 113 | 199 | 791 | 35439 |
| toDelivered | 49 | 140 | 231 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 6 | 0 |
| saleConsumerLag | 1 | 0 |
| notificationsPending | 55 | 0 |
| channelLag | 92 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T05:48:54.803Z |
| allReceivedAt | 2026-11-13T05:48:54.822Z |
| drainSeconds | 0.019 |
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
| parked | 1 |
