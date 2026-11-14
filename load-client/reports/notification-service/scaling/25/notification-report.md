# Notification report: scaling/25

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | scaling/25 |
| startedAt | 2026-11-13T11:53:53.6302369+00:00 |
| deploymentCommit | b51830368a6f4e373cafe0315c5fa865239928b8 |
| subscribers | 25 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":10,"durationSeconds":60} |
| faults | none |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 3222 |
| sales.activeWindowSeconds | 60.014 |
| sales.perSecond | 53.687 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 5802 |
| notifications.delivered | 5802 |
| notifications.received | 5802 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 96.461 |
| notifications.receivedPerSecond | 96.611 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 27 | 59 | 127 | 3928 | 5802 |
| toDelivered | 50 | 92 | 163 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 5 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 13 | 0 |
| channelLag | 26 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T11:56:46.941Z |
| allReceivedAt | 2026-11-13T11:56:46.977Z |
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
