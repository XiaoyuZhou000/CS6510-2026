# Notification report: scaling/50

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | scaling/50 |
| startedAt | 2026-11-13T11:57:09.7526453+00:00 |
| deploymentCommit | b51830368a6f4e373cafe0315c5fa865239928b8 |
| subscribers | 50 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":10,"durationSeconds":60} |
| faults | none |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 3793 |
| sales.activeWindowSeconds | 60.014 |
| sales.perSecond | 63.202 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 14449 |
| notifications.delivered | 14449 |
| notifications.received | 14449 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 240.661 |
| notifications.receivedPerSecond | 240.661 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 26 | 69 | 143 | 1965 | 14449 |
| toDelivered | 48 | 100 | 174 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 5 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 34 | 0 |
| channelLag | 47 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T12:00:03.318Z |
| allReceivedAt | 2026-11-13T12:00:03.343Z |
| drainSeconds | 0.025 |
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
