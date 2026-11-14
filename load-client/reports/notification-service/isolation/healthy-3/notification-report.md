# Notification report: isolation/healthy-3

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | isolation/healthy-3 |
| startedAt | 2026-11-13T12:38:57.7712252+00:00 |
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
| sales.completed | 3584 |
| sales.activeWindowSeconds | 60.024 |
| sales.perSecond | 59.709 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 33897 |
| notifications.delivered | 33897 |
| notifications.received | 33897 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 564.158 |
| notifications.receivedPerSecond | 564.541 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 27 | 98 | 182 | 4999 | 33897 |
| toDelivered | 49 | 131 | 215 | - | - |
| endToEndExcluding sub-0001 | 27 | 98 | 182 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 5 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 50 | 0 |
| channelLag | 95 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T12:41:25.639Z |
| allReceivedAt | 2026-11-13T12:41:25.668Z |
| drainSeconds | 0.029 |
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
