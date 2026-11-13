# Notification report: us6-unchanged/g-notification-stopped

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | us6-unchanged/g-notification-stopped |
| startedAt | 2026-11-13T10:09:28.9310467+00:00 |
| deploymentCommit | 4ee0f4fe207e34a9b119e998155fe743da9e955d |
| subscribers | 100 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":10,"durationSeconds":60} |
| faults | none |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 3872 |
| sales.activeWindowSeconds | 59.98 |
| sales.perSecond | 64.555 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 36737 |
| notifications.delivered | 36737 |
| notifications.received | 36737 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 0 |
| notifications.receivedPerSecond | 0 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 45510 | 72343 | 75599 | 78233 | 36737 |
| toDelivered | 45533 | 72363 | 75621 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 5 | 0 |
| saleConsumerLag | 868 | 0 |
| notificationsPending | 701 | 0 |
| channelLag | 146 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T10:12:04.492Z |
| allReceivedAt | 2026-11-13T10:12:25.218Z |
| drainSeconds | 20.726 |
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
