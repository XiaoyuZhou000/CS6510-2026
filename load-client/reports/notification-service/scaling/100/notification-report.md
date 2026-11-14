# Notification report: scaling/100

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | scaling/100 |
| startedAt | 2026-11-13T12:00:25.4561565+00:00 |
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
| sales.completed | 3841 |
| sales.activeWindowSeconds | 59.995 |
| sales.perSecond | 64.022 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 36370 |
| notifications.delivered | 36370 |
| notifications.received | 36370 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 605.567 |
| notifications.receivedPerSecond | 605.567 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 28 | 91 | 183 | 318 | 36370 |
| toDelivered | 50 | 125 | 219 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 5 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 69 | 0 |
| channelLag | 90 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T12:03:20.880Z |
| allReceivedAt | 2026-11-13T12:03:20.898Z |
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
