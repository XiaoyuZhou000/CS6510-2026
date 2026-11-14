# Notification report: cp2/stress-1

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | cp2/stress-1 |
| startedAt | 2026-11-13T11:46:39.9347863+00:00 |
| deploymentCommit | b51830368a6f4e373cafe0315c5fa865239928b8 |
| subscribers | 100 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":100,"durationSeconds":120} |
| faults | none |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 9784 |
| sales.activeWindowSeconds | 120.234 |
| sales.perSecond | 81.375 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 86499 |
| notifications.delivered | 86499 |
| notifications.received | 86499 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 719.214 |
| notifications.receivedPerSecond | 719.239 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 30 | 151 | 246 | 767 | 86499 |
| toDelivered | 49 | 189 | 297 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 21 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 161 | 0 |
| channelLag | 124 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T11:50:38.003Z |
| allReceivedAt | 2026-11-13T11:50:38.031Z |
| drainSeconds | 0.028 |
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
