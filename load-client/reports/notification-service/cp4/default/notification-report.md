# Notification report: cp4/default

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | cp4/default |
| startedAt | 2026-11-13T12:03:44.6078065+00:00 |
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
| sales.completed | 3523 |
| sales.activeWindowSeconds | 60.033 |
| sales.perSecond | 58.684 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 33520 |
| notifications.delivered | 33520 |
| notifications.received | 33520 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 557.427 |
| notifications.receivedPerSecond | 557.743 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 27 | 94 | 182 | 2512 | 33520 |
| toDelivered | 50 | 127 | 222 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 4 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 66 | 0 |
| channelLag | 97 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T12:06:41.004Z |
| allReceivedAt | 2026-11-13T12:06:41.024Z |
| drainSeconds | 0.02 |
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
