# Notification report: isolation/reject-2

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | isolation/reject-2 |
| startedAt | 2026-11-13T12:32:00.2429355+00:00 |
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
| sales.completed | 3896 |
| sales.activeWindowSeconds | 60.046 |
| sales.perSecond | 64.884 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 36848 |
| notifications.delivered | 36848 |
| notifications.received | 36848 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 608.683 |
| notifications.receivedPerSecond | 609.183 |
| deliveryFailures.chargedFailures | 193 |
| deliveryFailures.unchargedFailures | 2250 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 27 | 111 | 209 | 72633 | 36848 |
| toDelivered | 49 | 139 | 237 | - | - |
| endToEndExcluding sub-0001 | 27 | 98 | 172 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 6 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 279 | 0 |
| channelLag | 92 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T12:34:53.886Z |
| allReceivedAt | 2026-11-13T12:35:06.697Z |
| drainSeconds | 12.811 |
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
