# Sample: Airflow ETL modernization, vendor JSON on S3 to Postgres + dbt
subtitle: Sample on synthetic data

## The problem
Our Python ETL loads vendor JSON files from S3, and it fails quietly whenever a vendor changes a field. We want the data in Postgres with tests, readable monitoring and safe re-runs.

## What I built
- The old job kept as the "before", and an Airflow pipeline that replaces it: three vendor feeds and an orders export, from S3 into Postgres.
- A data contract for every feed. A file that breaks its contract goes to quarantine with a reason, and the other feeds still load.
- Loads keyed by file checksum, so a re-run never loads a file twice. Backfill by date.
- dbt models from staging to marts, with a SKU crosswalk that ties each vendor's product codes to order lines, plus a manual override list.
- A health report: freshness, row counts, rejected files with reasons, and how many order lines tie to a product.

[[diagram]]

## What the sample proves
- {{pytest_total}} automated tests pass, with {{coverage_pct}}% line coverage on the pipeline package (the gate is 85%).
- {{dbt_total}} dbt tests and unit tests pass on the built models.
- In the end-to-end run, the day-5 vendor file with a renamed field is quarantined and the other three feeds load.
- Re-running files that already loaded adds zero rows.
- Order totals from the new tables equal the old script's totals for all {{orders}} orders.
- {{match_pct}}% of order lines tie to a product in this run. The rest stay in the table, labeled unmatched.

## How a client engagement would run
Week one: read the current job and its recent failures, list each feed and what it promises, then move one feed end to end into Postgres with a contract, a quarantine and a re-run check. Week two: add the remaining feeds, the dbt models and tests, backfill, and the health report, then run old and new side by side and compare the totals before anything is switched off.

## Background
- At Broadridge's wealth-management business I owned client onboarding and the canonical Family/Member/Entity/Account data model, so matching one entity across systems is familiar ground.

footer: Kunjar Bhaduri. I lead and deliver the work myself, using frontier AI tooling to move fast.
