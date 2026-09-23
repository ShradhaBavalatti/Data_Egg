# Validation Report

## Layer counts
| Layer | Records |
| --- | --- |
| Source | 1200000 |
| Bronze | 1218213 |
| Silver | 1183524 |
| Quarantine | 22634 |
| Deduplicated | 12055 |
| fact_trips | 1183524 |
| Gold borough hourly | 1956 |
| Gold zone flows | 18141 |
| Gold payment daily | 56 |

## Reconciliation
`Bronze = Silver + Quarantine + Deduplicated`

`1218213 = 1183524 + 22634 + 12055`

## Silver quality checks
| Check | Result |
| --- | --- |
| no_null_trip_key | True |
| no_duplicate_trip_key | True |
| no_null_pickup_location | True |
| no_negative_fares | True |
| no_null_passenger_count | True |
| valid_pickup_locations | True |
| valid_dropoff_locations | True |
| silver_le_source | True |
| fact_equals_silver | True |
| reconciliation | True |

## Kafka monitoring
| Metric | Value |
| --- | --- |
| producer_rps | 5539.389008774943 |
| consumer_rps | 34156.23379676247 |
| final_lag | 0.0 |
| peak_lag | 1216213.0 |
| avg_lag | 607241.6573770492 |

**Overall: PASS**
