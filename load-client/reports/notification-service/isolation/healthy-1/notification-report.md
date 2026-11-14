# Notification report: isolation/healthy-1

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | isolation/healthy-1 |
| startedAt | 2026-11-13T12:19:41.2145364+00:00 |
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
| sales.completed | 3750 |
| sales.activeWindowSeconds | 59.956 |
| sales.perSecond | 62.546 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 36357 |
| notifications.delivered | 36357 |
| notifications.received | 36357 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 605.861 |
| notifications.receivedPerSecond | 606.295 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 26 | 96 | 169 | 1286 | 36357 |
| toDelivered | 47 | 123 | 199 | - | - |
| endToEndExcluding sub-0001 | 26 | 97 | 169 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 7 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 61 | 0 |
| channelLag | 92 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T12:22:05.784Z |
| allReceivedAt | 2026-11-13T12:22:05.803Z |
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
| parked | 0 |
