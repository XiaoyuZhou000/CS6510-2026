# Notification report: cp2/default-2

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | cp2/default-2 |
| startedAt | 2026-11-13T11:43:17.3259206+00:00 |
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
| sales.completed | 3643 |
| sales.activeWindowSeconds | 60.113 |
| sales.perSecond | 60.603 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 34316 |
| notifications.delivered | 34316 |
| notifications.received | 34316 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 569.727 |
| notifications.receivedPerSecond | 570.176 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 28 | 99 | 171 | 2068 | 34316 |
| toDelivered | 50 | 130 | 208 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 6 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 77 | 0 |
| channelLag | 98 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T11:46:16.844Z |
| allReceivedAt | 2026-11-13T11:46:16.862Z |
| drainSeconds | 0.018 |
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
