# Notification report: isolation/reject-1

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | isolation/reject-1 |
| startedAt | 2026-11-13T12:22:28.6374075+00:00 |
| deploymentCommit | b51830368a6f4e373cafe0315c5fa865239928b8 |
| subscribers | 100 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":10,"durationSeconds":60} |
| faults | reject:sub-0001 |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 3910 |
| sales.activeWindowSeconds | 59.99 |
| sales.perSecond | 65.178 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 36941 |
| notifications.delivered | 36941 |
| notifications.received | 36941 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 610.702 |
| notifications.receivedPerSecond | 611.185 |
| deliveryFailures.chargedFailures | 116 |
| deliveryFailures.unchargedFailures | 2359 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 27 | 103 | 214 | 69789 | 36941 |
| toDelivered | 48 | 137 | 255 | - | - |
| endToEndExcluding sub-0001 | 27 | 93 | 170 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 8 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 265 | 0 |
| channelLag | 93 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T12:25:22.700Z |
| allReceivedAt | 2026-11-13T12:25:35.535Z |
| drainSeconds | 12.835 |
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
