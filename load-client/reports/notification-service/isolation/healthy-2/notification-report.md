# Notification report: isolation/healthy-2

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | isolation/healthy-2 |
| startedAt | 2026-11-13T12:29:15.2379074+00:00 |
| deploymentCommit | b51830368a6f4e373cafe0315c5fa865239928b8 |
| subscribers | 100 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":10,"durationSeconds":60} |
| faults | none |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 3267 |
| sales.activeWindowSeconds | 59.986 |
| sales.perSecond | 54.463 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 31882 |
| notifications.delivered | 31882 |
| notifications.received | 31882 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 530.274 |
| notifications.receivedPerSecond | 530.59 |
| deliveryFailures.chargedFailures | 0 |
| deliveryFailures.unchargedFailures | 0 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 27 | 94 | 191 | 8662 | 31882 |
| toDelivered | 48 | 129 | 224 | - | - |
| endToEndExcluding sub-0001 | 27 | 94 | 191 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 5 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 73 | 0 |
| channelLag | 92 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T12:31:38.173Z |
| allReceivedAt | 2026-11-13T12:31:38.190Z |
| drainSeconds | 0.017 |
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
