# Sample Data Verification Guide

Verification steps for each dataset after loading all three batches of sample data.
Run these checks in order — each batch should be loaded and the pipeline run before
loading the next.

---

## Patients — SCD Type 1 (Upsert)

**Batch sizes:** 60 → 80 → 106 patients (20 new in batch 2, 26 new in batch 3)

After loading all three batches:

- `silver.patients` row count == **106** (final batch is a full snapshot of all patients)
- After batch 2: previously loaded patients with changed fields (expenses, address, marital status) must show the new values — old values must be gone (SCD1 keeps only current state)
- After batch 3: any further updates must be reflected; 26 additional patients must appear
- No `patient_id` appears more than once:
  ```sql
  SELECT patient_id, COUNT(*) FROM silver.patients GROUP BY 1 HAVING COUNT(*) > 1
  -- must return zero rows
  ```
- `SSN` and `ADDRESS` columns return masked values for users outside `pii-authorized`
- Reloading batch 1 after batch 3 must not roll back any updated records — the `_metadata.file_modification_time` sequence column must correctly reject stale files

---

## Encounters — Streaming (Append-Only)

**Batch sizes:** 1,643 / 1,643 / 1,644 records (non-overlapping, sorted by START date)

After loading all three batches:

- `silver.encounters` row count == **4,930** (all source rows, no duplicates)
- `encounter_start_ts` ranges across batches are non-overlapping — batch 1 holds the oldest, batch 3 the most recent
- No `encounter_id` appears more than once:
  ```sql
  SELECT encounter_id, COUNT(*) FROM silver.encounters GROUP BY 1 HAVING COUNT(*) > 1
  -- must return zero rows
  ```
- After loading batch 1 only, confirm the checkpoint is set — reloading batch 1 must not append duplicate rows

---

## Observations — Materialized View (Latest Vital Signs)

**Batch sizes:** 4,027 / 4,027 / 4,027 vital-sign records (12,081 total source rows, sorted by DATE)

After loading all three batches:

- `silver.latest_vitals` row count == number of distinct `(member_id, loinc_code)` pairs across the full dataset (each pair appears exactly once)
- `observation_ts` for every row is >= the most recent observation for that `(member_id, loinc_code)` across all source files:
  ```sql
  -- Should return zero rows
  SELECT v.member_id, v.loinc_code
  FROM silver.latest_vitals v
  JOIN (
      SELECT member_id, loinc_code, MAX(observation_ts) AS max_ts
      FROM silver.latest_vitals_source
      GROUP BY 1, 2
  ) s USING (member_id, loinc_code)
  WHERE v.observation_ts < s.max_ts
  ```
- After loading only batch 1, re-run the pipeline after dropping batch 2 — rows for patients with newer measurements in batch 2 must be updated to the batch 2 values (confirms the materialized view recomputes correctly)

---

## Providers — SCD Type 2 (History Tracking)

**Batch sizes:** 40 / 40 / 40 providers (full snapshot each batch — changes between snapshots drive history rows)

After loading all three batches:

- Each provider that changed across batches has multiple rows — one row per version
- Exactly one row per provider has `is_current = true` (or `end_date IS NULL`); all prior versions must have a non-null `end_date`:
  ```sql
  SELECT provider_id, COUNT(*) FROM silver.providers WHERE is_current GROUP BY 1 HAVING COUNT(*) > 1
  -- must return zero rows
  ```
- `effective_start` and `effective_end` date ranges for each provider are contiguous with no gaps and no overlaps
- Providers that never changed across all three batches have exactly one row
- Total row count > number of distinct provider IDs (the delta reflects change events captured across batches)
- Reloading batch 1 after all three batches must not create duplicate history rows — the pipeline must be idempotent
