# Consolidated Data Quality Report

Generated (UTC): `2026-11-12T15:14:55.425328+00:00`

## Overall Summary

| Metric | Value |
| --- | ---: |
| Batches Validated | 17 |
| Total Raw Rows | 541,909 |
| Valid Rows (Silver) | 534,135 |
| Rejected Rows (Quarantine) | 7,774 |
| Overall Pass Rate | 98.57% |

## Primary Rejection Reasons

| Rejection Code | Rejected Rows |
| --- | ---: |
| `DQ_EXACT_DUPLICATE` | 5,256 |
| `DQ_NON_POSITIVE_UNIT_PRICE` | 2,515 |
| `DQ_NON_STANDARD_INVOICE` | 3 |

## All Rule Violations (Non-Exclusive)

| Rule Code | Violation Count |
| --- | ---: |
| `DQ_EXACT_DUPLICATE` | 5,261 |
| `DQ_NON_POSITIVE_UNIT_PRICE` | 2,517 |
| `DQ_NEGATIVE_QTY_WITHOUT_CANCEL` | 1,336 |
| `DQ_NON_STANDARD_INVOICE` | 3 |

## Silver Zone Business Characteristics

| Indicator | Valid Rows | Handling Strategy |
| --- | ---: | --- |
| Cancelled Invoices (`C` prefix) | 9,251 | Retained with `is_cancelled = true` for net revenue & cancellation analytics |
| Missing `CustomerID` | 132,564 | Retained with `is_guest_customer = true` mapped to `CustomerKey = -1` |
| Missing `Description` | 0 | Normalized to `'UNKNOWN PRODUCT'` with flag |
| `Unspecified` Country | 442 | Preserved in `dim_country` for full reconciliation |

## Per-Batch Summary

| Batch ID | Total | Valid | Rejected | Pass Rate |
| --- | ---: | ---: | ---: | ---: |
| `batch_0001_historical_backfill` | 491,984 | 485,030 | 6,954 | 98.59% |
| `batch_0002_daily_2011-11-23` | 3,619 | 3,550 | 69 | 98.09% |
| `batch_0003_daily_2011-11-24` | 3,766 | 3,701 | 65 | 98.27% |
| `batch_0004_daily_2011-11-25` | 3,125 | 3,062 | 63 | 97.98% |
| `batch_0005_daily_2011-11-27` | 2,543 | 2,428 | 115 | 95.48% |
| `batch_0006_daily_2011-11-28` | 3,330 | 3,275 | 55 | 98.35% |
| `batch_0007_daily_2011-11-29` | 4,313 | 4,233 | 80 | 98.15% |
| `batch_0008_daily_2011-11-30` | 3,454 | 3,417 | 37 | 98.93% |
| `batch_0009_daily_2011-12-01` | 2,901 | 2,880 | 21 | 99.28% |
| `batch_0010_daily_2011-12-02` | 2,880 | 2,825 | 55 | 98.09% |
| `batch_0011_daily_2011-12-04` | 2,038 | 1,980 | 58 | 97.15% |
| `batch_0012_daily_2011-12-05` | 5,331 | 5,284 | 47 | 99.12% |
| `batch_0013_daily_2011-12-06` | 3,365 | 3,310 | 55 | 98.37% |
| `batch_0014_daily_2011-12-07` | 2,438 | 2,387 | 51 | 97.91% |
| `batch_0015_daily_2011-12-08` | 4,940 | 4,901 | 39 | 99.21% |
| `batch_0016_daily_2011-12-09` | 1,632 | 1,625 | 7 | 99.57% |
| `batch_9999_late_arriving` | 250 | 247 | 3 | 98.80% |
