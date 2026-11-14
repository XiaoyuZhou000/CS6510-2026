# Notification report: isolation/reject-3

Reconciliation: **PASS**. Drain deadline (60 s) met: **true**.

## Run

| Field | Value |
|---|---|
| id | isolation/reject-3 |
| startedAt | 2026-11-13T12:41:51.4494484+00:00 |
| deploymentCommit | b51830368a6f4e373cafe0315c5fa865239928b8 |
| subscribers | 100 |
| seed | 20261111 |
| skusPerSubscriber | 20 |
| skuDistribution | uniform |
| loadClient | {"stations":10,"durationSeconds":60} |
| faults | reject:sub-0001 |

## Sales and notifications

| Metric | Value |
|---|---|
| sales.completed | 3645 |
| sales.activeWindowSeconds | 60.181 |
| sales.perSecond | 60.567 |
| sales.backgroundCompletions | 0 |
| notifications.owed | 35235 |
| notifications.delivered | 35235 |
| notifications.received | 35235 |
| notifications.redeliveries | 0 |
| notifications.deliveredPerSecond | 581.163 |
| notifications.receivedPerSecond | 581.163 |
| deliveryFailures.chargedFailures | 177 |
| deliveryFailures.unchargedFailures | 1867 |
| deliveryFailures.parked | 0 |
| deliveryFailures.operatorRetried | 0 |

## Latency (ms, nearest rank)

| Measure | p50 | p95 | p99 | max | n |
|---|---|---|---|---|---|
| endToEnd | 27 | 106 | 187 | 70667 | 35235 |
| toDelivered | 49 | 136 | 226 | - | - |
| endToEndExcluding sub-0001 | 27 | 100 | 172 | - | - |

## Backlog

| Field | max | final |
|---|---|---|
| saleEventsUnpublished | 6 | 0 |
| saleConsumerLag | 0 | 0 |
| notificationsPending | 290 | 0 |
| channelLag | 89 | 0 |

## Drain

| Field | Value |
|---|---|
| finalCompletionAt | 2026-11-13T12:44:50.035Z |
| allReceivedAt | 2026-11-13T12:45:03.147Z |
| drainSeconds | 13.112 |
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
