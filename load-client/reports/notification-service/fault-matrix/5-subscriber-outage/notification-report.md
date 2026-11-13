# Notification report: fault-matrix/5-subscriber-outage

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | fault-matrix/5-subscriber-outage |
| startedAt | 2026-11-13T06:02:55.1252658+00:00 |
| deploymentCommit | bc85a457228d41b0982cb055686942f14c4018c9 |
| subscribers | 100 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":10,"durationSeconds":60} |
| faults | none |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 3842 |
| sales.activeWindowSeconds | 60.128 |
| sales.perSecond | 63.897 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 36339 |
| notifications.delivered | 36339 |
| notifications.received | 36339 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 602.797 |
| notifications.receivedPerSecond | 603.712 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 3006 | 29922 | 32198 | 32944 | 36339 |
| toDelivered | 49 | 127 | 189 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 7 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 53 | 0 |
| channelLag | 2819 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T06:05:45.470Z |
| allReceivedAt | 2026-11-13T06:05:45.493Z |
| drainSeconds | 0.023 |
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
