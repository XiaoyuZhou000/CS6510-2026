# Notification report: scaling/10

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | scaling/10 |
| startedAt | 2026-11-13T11:51:07.3016302+00:00 |
| deploymentCommit | b51830368a6f4e373cafe0315c5fa865239928b8 |
| subscribers | 10 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":10,"durationSeconds":60} |
| faults | none |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 3797 |
| sales.activeWindowSeconds | 60.012 |
| sales.perSecond | 63.271 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 3152 |
| notifications.delivered | 3152 |
| notifications.received | 3152 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 52.506 |
| notifications.receivedPerSecond | 52.523 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 28 | 64 | 137 | 449 | 3152 |
| toDelivered | 54 | 92 | 169 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 5 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 9 | 0 |
| channelLag | 13 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T11:53:31.364Z |
| allReceivedAt | 2026-11-13T11:53:31.349Z |
| drainSeconds | 0 |
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
