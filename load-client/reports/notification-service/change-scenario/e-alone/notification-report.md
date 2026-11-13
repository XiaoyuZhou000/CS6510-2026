# Notification report: e-alone

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | e-alone |
| startedAt | - |
| deploymentCommit | - |
| subscribers | 12 |
| seed | - |
| skusPerSubscriber | - |
| skuDistribution | - |
| loadClient | {"stations":null,"durationSeconds":null} |
| faults | none |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 63 |
| sales.activeWindowSeconds | 3.546 |
| sales.perSecond | 17.766 |
| sales.backgroundCompletions | - |
| notifications.owed | 212 |
| notifications.delivered | 212 |
| notifications.received | 212 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 57.812 |
| notifications.receivedPerSecond | 57.812 |
| deliveryFailures.chargedFailures | 1 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 31 | 47 | 76 | 11478 | 212 |
| toDelivered | 59 | 74 | 104 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | - | - |
| saleConsumerLag | - | - |
| notificationsPending | - | - |
| channelLag | - | - |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T01:33:46.562Z |
| allReceivedAt | 2026-11-13T01:33:54.680Z |
| drainSeconds | 8.118 |
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
