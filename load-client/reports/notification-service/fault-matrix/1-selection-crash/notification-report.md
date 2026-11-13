# Notification report: fault-matrix/1-selection-crash

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | fault-matrix/1-selection-crash |
| startedAt | 2026-11-13T06:06:46.2889379+00:00 |
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
| sales.completed | 3170 |
| sales.activeWindowSeconds | 60.12 |
| sales.perSecond | 52.728 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 30770 |
| notifications.delivered | 30770 |
| notifications.received | 30770 |
| notifications.redeliveries | 15 |
| notifications.deliveredPerSecond | 511.56 |
| notifications.receivedPerSecond | 511.56 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 30 | 11349 | 13101 | 13967 | 30770 |
| toDelivered | 54 | 11373 | 13146 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 5 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 68 | 0 |
| channelLag | 117 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T06:09:09.553Z |
| allReceivedAt | 2026-11-13T06:09:09.576Z |
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
