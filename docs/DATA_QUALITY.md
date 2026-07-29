# Mock-data quality contract

The generator deliberately creates imperfect source data so the Spark job has
real cleansing and quarantine work to perform. Its default 5% base rate is
scaled by source and issue severity: formatting/missing optional fields are
common, while financial, cross-source, and timeline errors are rare. MongoDB
legacy fields are clustered by restaurant to model an older producer version.
Use `--dirty-rate 0` to generate a clean control dataset.

## Expected source issues

| Source | Deliberate issues | Suggested ETL action |
|---|---|---|
| Customers | Email, name, or city formatting | Trim and normalize while requiring unique email and a non-null phone |
| Restaurants/drivers | Missing restaurant address and inconsistent category, status, name, or plate formatting | Trim, normalize, require a non-null driver phone and unique plate, and quarantine invalid identifiers |
| MongoDB menus | Missing optional description, numeric strings, legacy field name | Apply schema evolution and safe casts |
| Order items | Unknown cross-source menu ID, incorrect line total | Quarantine unknown IDs; recompute totals |
| Orders | Status/address formatting, incorrect total, late update | Normalize status/address, recompute total, use watermark/lookback |
| Payments | Missing transaction reference, method formatting, amount mismatch | Validate by method and reconcile to order |
| Deliveries | Impossible distance, timestamp ordering, status formatting | Range-check and quarantine invalid timelines |

`generation_manifest.json` records exact injected-anomaly counts for each run.
Business-null fields such as `paid_at` on a pending payment are expected and are
not counted as anomalies.
