# Notification report: us3-smoke/d-default-2

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | us3-smoke/d-default-2 |
| startedAt | 2026-11-12T23:12:03.5076029+00:00 |
| deploymentCommit | 423193bb403baba7eafc7f32d792224a234781af |
| subscribers | 100 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":10,"durationSeconds":60} |
| faults | none |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 3917 |
| sales.activeWindowSeconds | 59.914 |
| sales.perSecond | 65.377 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 37414 |
| notifications.delivered | 37414 |
| notifications.received | 37414 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 623.761 |
| notifications.receivedPerSecond | 624.111 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 26 | 92 | 157 | 274 | 37414 |
| toDelivered | 48 | 123 | 183 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 6 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 60 | 0 |
| channelLag | 94 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-12T23:15:16.057Z |
| allReceivedAt | 2026-11-12T23:15:16.080Z |
| drainSeconds | 0.023 |
| deadlineSeconds | 60 |
| deadlineMet | true |

## Reconciliation counts

| Category | Count |
|---|---|
| saleEvents | 0 |
| fixedSubscriptions | 0 |
| selectionMismatch | 0 |
| missingNotDelivered | 0 |
| missingDeliveredNotReceived | 0 |
| unexpected | 0 |
| duplicate | 0 |
| quantityMismatch | 0 |
| parked | 0 |
