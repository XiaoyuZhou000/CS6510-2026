# Notification report: fault-matrix/4-broker-outage

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | fault-matrix/4-broker-outage |
| startedAt | 2026-11-13T05:59:08.7294583+00:00 |
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
| sales.completed | 1280 |
| sales.activeWindowSeconds | 64.721 |
| sales.perSecond | 19.777 |
| sales.backgroundCompletions | 50 |
| notifications.owed | 12512 |
| notifications.delivered | 12512 |
| notifications.received | 12512 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 187.049 |
| notifications.receivedPerSecond | 185.689 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 29 | 147 | 2576 | 3667 | 12512 |
| toDelivered | 52 | 150 | 536 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 6 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 45 | 0 |
| channelLag | 84 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T06:02:31.637Z |
| allReceivedAt | 2026-11-13T06:02:34.440Z |
| drainSeconds | 2.803 |
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
