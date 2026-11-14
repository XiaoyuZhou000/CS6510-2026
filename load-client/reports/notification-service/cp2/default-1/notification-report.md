# Notification report: cp2/default-1

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | cp2/default-1 |
| startedAt | 2026-11-13T11:40:20.1438488+00:00 |
| deploymentCommit | b51830368a6f4e373cafe0315c5fa865239928b8 |
| subscribers | 100 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":10,"durationSeconds":60} |
| faults | none |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 3840 |
| sales.activeWindowSeconds | 60.231 |
| sales.perSecond | 63.755 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 36189 |
| notifications.delivered | 36189 |
| notifications.received | 36189 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 600.322 |
| notifications.receivedPerSecond | 600.571 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 27 | 85 | 167 | 423 | 36189 |
| toDelivered | 50 | 119 | 200 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 6 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 86 | 0 |
| channelLag | 92 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T11:42:53.472Z |
| allReceivedAt | 2026-11-13T11:42:53.507Z |
| drainSeconds | 0.035 |
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
