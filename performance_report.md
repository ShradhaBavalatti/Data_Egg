# Performance Report

## Workload
Fixed bounded source slice; identical workload and clean lake for each profile.

## Head-to-head
| Metric | V1 baseline | V2 optimised | V2 gain |
| --- | --- | --- | --- |
| Producer throughput | 1714.0397478289922 | 4997.134069773588 | 191.5% |
| Spark throughput | 1589.9 | 8367.0 | 426.3% |
| Spark avg batch latency | 12765.6 | 12129.0 | 5.2% |
| End-to-end wall-clock | 219.89206560000093 | 65.2623151000007 | 236.9% |
| Final consumer lag | 0.0 | 0.0 | — |

## What changed in V2
| Lever | V1 | V2 |
|---|---:|---:|
| Shuffle partitions | 200 | 8 |
| Max files/trigger | 1 | 16 |
| Broadcast zone join | No | Yes |
| Kafka batch size | 16 KB | 64 KB |
| Kafka linger | 0 ms | 50 ms |
| Kafka compression | none | gzip |

## Raw metrics
| profile | producer_rps | consumer_rps | consumer_lag | spark_avg_batch_ms | spark_rows_per_sec | silver_rows | end_to_end_sec |
| --- | --- | --- | --- | --- | --- | --- | --- |
| v1 | 1714.0397478289922 | 10724.22645798473 | 0 | 12765.6 | 1589.9 | 101483 | 219.89206560000093 |
| v2 | 4997.134069773588 | 11547.471825877388 | 0 | 12129.0 | 8367.0 | 101483 | 65.2623151000007 |
