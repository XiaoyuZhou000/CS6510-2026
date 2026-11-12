# Notification report: us3-smoke/d-default-1

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | us3-smoke/d-default-1 |
| startedAt | 2026-11-12T07:46:13.6527849+00:00 |
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
| sales.completed | 3911 |
| sales.activeWindowSeconds | 60.082 |
| sales.perSecond | 65.094 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 37446 |
| notifications.delivered | 37446 |
| notifications.received | 37446 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 622.05 |
| notifications.receivedPerSecond | 0 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 27 | 101 | 159 | 302 | 37446 |
| toDelivered | 49 | 132 | 196 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 4 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 77 | 0 |
| channelLag | 92 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-12T15:49:07.125Z |
| allReceivedAt | 2026-11-12T07:49:07.160Z |
| drainSeconds | 0 |
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
