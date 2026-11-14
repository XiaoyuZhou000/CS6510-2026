# Notification report: isolation/stall-1

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | isolation/stall-1 |
| startedAt | 2026-11-13T12:26:10.0413438+00:00 |
| deploymentCommit | b51830368a6f4e373cafe0315c5fa865239928b8 |
| subscribers | 100 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":10,"durationSeconds":60} |
| faults | stall:sub-0001 |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 3835 |
| sales.activeWindowSeconds | 60.06 |
| sales.perSecond | 63.853 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 36284 |
| notifications.delivered | 36284 |
| notifications.received | 36284 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 599.883 |
| notifications.receivedPerSecond | 599.883 |
| deliveryFailures.chargedFailures | 361 |
| deliveryFailures.unchargedFailures | 44 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 27 | 107 | 195 | 73940 | 36284 |
| toDelivered | 49 | 137 | 227 | - | - |
| endToEndExcluding sub-0001 | 27 | 100 | 167 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 5 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 256 | 0 |
| channelLag | 93 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T12:28:36.965Z |
| allReceivedAt | 2026-11-13T12:28:57.301Z |
| drainSeconds | 20.336 |
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
