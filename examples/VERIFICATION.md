# Sample Data Verification Guide

Verification steps for each dataset after loading all three batches of sample data.
Load each batch, run the pipeline, then run the checks before loading the next batch.

Replace `enterprise_dev` with your target catalog (e.g. `enterprise_dev`).

---

## Patients — SCD Type 1 (Upsert)

**Silver table:** `enterprise_dev.silver.patients`  
**Batch sizes:** 60 → 80 → 106 (20 new patients in batch 2, 26 new in batch 3)

What changes between batches on the **existing** rows:
- `healthcare_expenses` and `healthcare_coverage` are refreshed on **every** existing row in every batch — SCD1 must always show the latest value
- `address`, `city`, `county`, `zip`: 3 rows changed per batch transition
- `death_date`: 3 rows changed per batch transition
- `marital_status`: 2 rows changed per batch transition

### After loading batch 1 (60 patients)

```sql
-- Row count
SELECT COUNT(*) FROM enterprise_dev.silver.patients;
-- Expected: 60

-- No duplicate member_id
SELECT member_id, COUNT(*) AS n
FROM enterprise_dev.silver.patients
GROUP BY member_id
HAVING n > 1;
-- Expected: 0 rows
```

### After loading batch 2 (80 patients)

```sql
-- Row count reflects 20 new patients
SELECT COUNT(*) FROM enterprise_dev.silver.patients;
-- Expected: 80

-- All 60 existing patients show updated financials (not the batch-1 values)
SELECT COUNT(*) FROM enterprise_dev.silver.patients
WHERE healthcare_expenses = 0 OR healthcare_coverage = 0;
-- Expected: 0 (all patients have non-zero updated values)

-- Verify 2 patients now have marital_status = 'M' that did not before
-- (run this before and after batch 2 load and compare)
SELECT COUNT(*) FROM enterprise_dev.silver.patients WHERE marital_status = 'M';
```

### After loading batch 3 (106 patients)

```sql
-- Final row count
SELECT COUNT(*) FROM enterprise_dev.silver.patients;
-- Expected: 106

-- No duplicate member_id at any point
SELECT member_id, COUNT(*) AS n
FROM enterprise_dev.silver.patients
GROUP BY member_id
HAVING n > 1;
-- Expected: 0 rows

-- SSN masking: non-pii-authorized users should see '***-**-XXXX' format
SELECT DISTINCT LEFT(ssn, 7) FROM enterprise_dev.silver.patients LIMIT 5;
-- Expected: '***-**-' for users outside the pii-authorized group

-- Address masking: non-pii-authorized users should see '*** MASKED ***'
SELECT DISTINCT address FROM enterprise_dev.silver.patients LIMIT 5;
-- Expected: '*** MASKED ***' for users outside the pii-authorized group

-- Sequence column correctness: reload batch 1 into the Volume and re-run the
-- pipeline — financials must NOT roll back to batch-1 values
SELECT COUNT(*) FROM enterprise_dev.silver.patients;
-- Expected: still 106 (no rows lost, no values regressed)
```

---

## Encounters — Streaming (Append-Only)

**Silver table:** `enterprise_dev.silver.encounters`  
**Batch sizes:** 1,643 / 1,643 / 1,644 (non-overlapping by `encounter_start_ts`, sorted chronologically)

| Batch | `encounter_start_ts` range |
|---|---|
| 2024-01-01 | 1938-06-15 → 2018-11-17 |
| 2024-02-01 | 2018-11-18 → 2022-12-19 |
| 2024-03-01 | 2022-12-19 → 2026-05-13 |

### After loading all three batches

```sql
-- Total row count
SELECT COUNT(*) FROM enterprise_dev.silver.encounters;
-- Expected: 4,930

-- No duplicate encounter_id across any batch
SELECT encounter_id, COUNT(*) AS n
FROM enterprise_dev.silver.encounters
GROUP BY encounter_id
HAVING n > 1;
-- Expected: 0 rows

-- Verify chronological coverage — batches must cover distinct date ranges
SELECT
  MIN(encounter_start_ts) AS earliest,
  MAX(encounter_start_ts) AS latest
FROM enterprise_dev.silver.encounters;
-- Expected: 1938-06-15 to 2026-05-13

-- Checkpoint idempotency: drop batch-1 files back into the Volume and re-run
SELECT COUNT(*) FROM enterprise_dev.silver.encounters;
-- Expected: still 4,930 (streaming checkpoint prevents re-processing)
```

---

## Providers — SCD Type 2 (Full History)

**Silver table:** `enterprise_dev.silver.providers`  
**Batch sizes:** 40 / 40 / 40 (same 40 provider IDs across all batches — no new providers)

What changes between batches:
- **Batch 1 → 2:** 6 providers had `organization_id` changed
- **Batch 2 → 3:** 6 *different* providers had `address` and `specialty` changed
- 12 unique providers changed across the full dataset

Databricks SCD Type 2 system columns: `__START_AT`, `__END_AT`, `__IS_CURRENT`

### After loading all three batches

```sql
-- Total rows (40 current + 6 historical from B1→B2 + 6 historical from B2→B3)
SELECT COUNT(*) FROM enterprise_dev.silver.providers;
-- Expected: 52

-- Exactly 40 current records
SELECT COUNT(*) FROM enterprise_dev.silver.providers
WHERE __IS_CURRENT = TRUE;
-- Expected: 40

-- No provider has more than one current row
SELECT provider_id, COUNT(*) AS n
FROM enterprise_dev.silver.providers
WHERE __IS_CURRENT = TRUE
GROUP BY provider_id
HAVING n > 1;
-- Expected: 0 rows

-- 12 providers have a history row (changed at least once)
SELECT provider_id, COUNT(*) AS versions
FROM enterprise_dev.silver.providers
GROUP BY provider_id
HAVING versions > 1;
-- Expected: 12 rows (each with versions = 2)

-- History is contiguous — no gaps between __START_AT and __END_AT
SELECT p1.provider_id
FROM enterprise_dev.silver.providers p1
JOIN enterprise_dev.silver.providers p2
  ON p1.provider_id = p2.provider_id
  AND p1.__END_AT != p2.__START_AT
  AND p1.__END_AT IS NOT NULL
  AND p1.__START_AT < p2.__START_AT;
-- Expected: 0 rows

-- Verify B1→B2 change: 6 providers changed organization_id
SELECT provider_id, organization_id, __START_AT, __END_AT, __IS_CURRENT
FROM enterprise_dev.silver.providers
WHERE provider_id IN (
  SELECT provider_id FROM enterprise_dev.silver.providers
  GROUP BY provider_id HAVING COUNT(*) > 1
)
ORDER BY provider_id, __START_AT;
```

---

## Observations — Materialized View (Latest Vital Sign per Patient per LOINC)

**Silver table:** `enterprise_dev.silver.observations`  
**Batch sizes:** 4,027 / 4,027 / 4,027 (12,081 total source rows — all are vital-signs, all are `type = numeric`)

The WHERE clause (`type = 'numeric'`) and QUALIFY clause (`ROW_NUMBER() ... = 1`) mean the silver
table holds exactly one row per `(member_id, loinc_code)` pair — the most recent reading.

| Batch | `observation_ts` range |
|---|---|
| 2024-01-01 | 1982-08-26 → 2020-04-24 |
| 2024-02-01 | 2020-04-25 → 2023-05-01 |
| 2024-03-01 | 2023-05-01 → 2026-05-13 |

### After loading all three batches

```sql
-- Silver row count equals distinct (member_id, loinc_code) pairs in the source
SELECT COUNT(*) FROM enterprise_dev.silver.observations;
-- Expected: 1,025

-- One row per (member_id, loinc_code) — no duplicates
SELECT member_id, loinc_code, COUNT(*) AS n
FROM enterprise_dev.silver.observations
GROUP BY member_id, loinc_code
HAVING n > 1;
-- Expected: 0 rows

-- All rows passed the numeric filter
SELECT COUNT(*) FROM enterprise_dev.silver.observations
WHERE type != 'numeric';
-- Expected: 0

-- observation_ts for every row is the latest across all source batches
-- (sample spot-check for one patient)
SELECT o.member_id, o.loinc_code, o.observation_ts AS silver_ts, src.max_ts
FROM enterprise_dev.silver.observations o
JOIN (
  SELECT member_id, loinc_code, MAX(observation_ts) AS max_ts
  FROM enterprise_dev.bronze.observations
  WHERE type = 'numeric'
  GROUP BY member_id, loinc_code
) src USING (member_id, loinc_code)
WHERE o.observation_ts < src.max_ts;
-- Expected: 0 rows

-- Materialized view refresh: after dropping batch 2 into the Volume and
-- re-running, rows for patients with newer observations in batch 2 must
-- update — query the same member_id before and after to confirm
SELECT observation_ts FROM enterprise_dev.silver.observations
WHERE member_id = '<any_member_id>' AND loinc_code = '<any_loinc_code>';
-- Expected: timestamp advances with each new batch that contains a newer reading
```
