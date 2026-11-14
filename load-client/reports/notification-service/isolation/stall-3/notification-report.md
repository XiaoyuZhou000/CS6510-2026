# Notification report: isolation/stall-3

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | isolation/stall-3 |
| startedAt | 2026-11-14T00:14:06.3781128+00:00 |
| deploymentCommit | b51830368a6f4e373cafe0315c5fa865239928b8 |
| subscribers | 100 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":10,"durationSeconds":60} |
| faults | stall:sub-0001 |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 4045 |
| sales.activeWindowSeconds | 60.071 |
| sales.perSecond | 67.337 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 38642 |
| notifications.delivered | 38642 |
| notifications.received | 38642 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 638.611 |
| notifications.receivedPerSecond | 638.811 |
| deliveryFailures.chargedFailures | 369 |
| deliveryFailures.unchargedFailures | 65 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 26 | 92 | 173 | 76033 | 38642 |
| toDelivered | 47 | 125 | 210 | - | - |
| endToEndExcluding sub-0001 | 26 | 85 | 154 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 5 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 313 | 0 |
| channelLag | 92 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-14T00:16:26.689Z |
| allReceivedAt | 2026-11-14T00:16:48.771Z |
| drainSeconds | 22.082 |
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
