# Notification report: isolation/stall-2

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | isolation/stall-2 |
| startedAt | 2026-11-13T12:35:25.8313476+00:00 |
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
| sales.completed | 3716 |
| sales.activeWindowSeconds | 60.246 |
| sales.perSecond | 61.68 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 35866 |
| notifications.delivered | 35866 |
| notifications.received | 35866 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 590.296 |
| notifications.receivedPerSecond | 590.877 |
| deliveryFailures.chargedFailures | 356 |
| deliveryFailures.unchargedFailures | 62 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 27 | 100 | 217 | 73327 | 35866 |
| toDelivered | 48 | 140 | 241 | - | - |
| endToEndExcluding sub-0001 | 27 | 94 | 179 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 7 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 285 | 0 |
| channelLag | 94 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T12:38:20.550Z |
| allReceivedAt | 2026-11-13T12:38:41.072Z |
| drainSeconds | 20.522 |
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
