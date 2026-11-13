# Notification report: e-with-load

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | e-with-load |
| startedAt | - |
| deploymentCommit | - |
| subscribers | 12 |
| seed | - |
| skusPerSubscriber | - |
| skuDistribution | - |
| loadClient | {"stations":null,"durationSeconds":null} |
| faults | none |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 3953 |
| sales.activeWindowSeconds | 67.107 |
| sales.perSecond | 58.906 |
| sales.backgroundCompletions | - |
| notifications.owed | 334 |
| notifications.delivered | 334 |
| notifications.received | 334 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 4.798 |
| notifications.receivedPerSecond | 4.888 |
| deliveryFailures.chargedFailures | 1 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 33 | 49 | 732 | 10944 | 334 |
| toDelivered | 62 | 78 | 759 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | - | - |
| saleConsumerLag | - | - |
| notificationsPending | - | - |
| channelLag | - | - |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T01:35:39.665Z |
| allReceivedAt | 2026-11-13T01:35:39.691Z |
| drainSeconds | 0.026 |
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
