# Notification report: cp4/stress

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | cp4/stress |
| startedAt | 2026-11-13T12:07:03.9396847+00:00 |
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
| sales.completed | 9210 |
| sales.activeWindowSeconds | 120.464 |
| sales.perSecond | 76.454 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 83393 |
| notifications.delivered | 83393 |
| notifications.received | 83393 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 692.215 |
| notifications.receivedPerSecond | 692.215 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 29 | 154 | 250 | 958 | 83393 |
| toDelivered | 49 | 191 | 309 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 21 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 110 | 0 |
| channelLag | 108 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T12:11:03.159Z |
| allReceivedAt | 2026-11-13T12:11:03.195Z |
| drainSeconds | 0.036 |
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
