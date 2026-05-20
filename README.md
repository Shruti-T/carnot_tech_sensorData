# Carnot Take-Home — Parcel Data Pipeline

## 1. Data Quality Audit

### parcel_readings.csv (3,447 raw rows)

---

#### Issue 1 — Mixed date formats

**What:** Three distinct formats coexist in the `date` column:

- ISO (`2026-01-27`) — 2,431 rows (70.5%)
- `DD/MM/YYYY` (`10/02/2026`) — 686 rows (19.9%)
- `DD-Mon-YYYY` (`20-Jan-2026`) — 330 rows (9.5%)

**Prevalence:** 29.5% of rows use a non-ISO format.  
**Decision — Repair.** Parse all three formats in sequence. This is a systematic upstream issue (likely multiple feed sources) and repairing it is strictly better to maintain common formatting in all downstream jobs.

---

#### Issue 2 — `sensor_status` case/whitespace inconsistency

**What:** The flag column has distinct raw values: `OK`, `error`, and `NaN` and 137 rows (4.0%) have a missing `sensor_status`.

**Decision — Repair.** Strip whitespace and upper-case. This collapses everything to three canonical values: `OK` (3,064), `ERROR` (246), `UNKNOWN` (137 formerly-null). This is a trivially safe normalisation.
Flag as UNKNOWN.\*\* We don't know if the sensor was healthy; treating them as `OK` would silently pollute NDVI analysis. Treating them as `ERROR` and dropping readings would lose legitimate data. Flagging as `UNKNOWN` preserves the row for non-NDVI uses (temperature, rainfall) while excluding them from quality-filtered NDVI analysis downstream.

---

#### Issue 3 — NDVI values outside physical range `[-1, 1]`

**What:** 104 rows (3.0%) have `ndvi_value` outside the valid range; values go as low as −1.977 and as high as 1.997. These are not plausible NDVI readings — they indicate sensor faults or unit errors.  
**Decision — Null and flag.** Set `ndvi_value = NaN` for out-of-range rows and add an `ndvi_flag` column (`ok` / `out_of_range`). Dropping the whole row would discard valid temperature/rainfall readings on the same day. Imputation would invent vegetation data we don't have evidence for.

---

#### Issue 5 — Duplicate `(parcel_id × date)` rows

**What:** 8 duplicate pairs (16 rows) share the same parcel + date. Inspection shows near-identical readings — same temperature and rainfall, NDVI jitter ≤ 0.04 — consistent with double-ingestion of the same day's feed rather than two genuinely distinct measurements.  
**Decision — Drop, keep first.** The jitter is within sensor noise; no information is gained by averaging. Keeping first is deterministic and reversible.

---

#### Issue 6 — Orphan parcel IDs (readings ↔ metadata mismatch)

**What:** Two parcel IDs appear in readings but have no metadata entry:

- `PARCEL_098` — 20 rows
- `PARCEL_099` — 20 rows

**Prevalence:** 40 rows (1.2%).  
**Decision — Retain with flag.** These rows are preserved in the output with `has_metadata = False`. They are excluded from any join-dependent analysis (NDVI by crop type, mill-level aggregations) but we don't silently destroy time-series data that could be back-filled once the metadata is located. A warning is emitted at runtime.

---

#### No issues found

- **Rainfall:** No negative values.
- **Temperature:** Range 15.1–35.5 °C — plausible for the agricultural geographies represented.

---

### parcel_metadata.csv (28 raw rows)

---

#### Issue 7 — Orphan metadata parcels (metadata ↔ readings mismatch)

**What:** Three parcel IDs exist in metadata but have zero rows in readings:

- `PARCEL_050`, `PARCEL_051`, `PARCEL_052`

**Decision — Log and exclude from output.** With no time-series rows, these parcels have nothing to anchor in the joined dataset. They are logged as warnings so the team can investigate whether readings were lost upstream.

---

#### No other issues found

- `sowing_date` is uniformly ISO-formatted; no nulls.
- `area_hectares` is fully populated with positive values (range 1.55–10.19 ha).
- `crop_type` is internally consistent across three values: `sugarcane`, `soybean`, `wheat`.
- `mill_id` is fully populated.

---

### Summary table

| #   | Issue                         | Rows affected | %     | Decision                       |
| --- | ----------------------------- | ------------- | ----- | ------------------------------ |
| 1   | Mixed date formats            | 1,016         | 29.5% | Repair (multi-format parse)    |
| 2   | sensor_status case/whitespace | ~3,310        | ~96%  | Repair (strip + upper)         |
| 3   | sensor_status null            | 137           | 4.0%  | Flag as UNKNOWN                |
| 4   | NDVI out of range             | 104           | 3.0%  | Null + flag column             |
| 5   | Duplicate rows                | 16 (8 pairs)  | 0.5%  | Drop, keep first               |
| 6   | Orphan reading parcels        | 40            | 1.2%  | Retain with has_metadata=False |
| 7   | Orphan metadata parcels       | 3 parcels     | —     | Log + exclude                  |

---

## 2. Pipeline

See `pipeline.py`. Key design choices:

- **No silent drops.** Every cleaning decision emits a log line with a count.
- **Provenance columns.** `ndvi_flag` and `has_metadata` travel with the output so downstream consumers know why a field is null.
- **Idempotent.** Running twice on the same input produces identical output.
- **Separation of concerns.** `clean_readings`, `clean_metadata`, `join`, and `sowing_window_analysis` are independent functions — easy to unit-test in isolation.

Output schema (12 columns):

```
parcel_id, date, ndvi_value, temperature_c, rainfall_mm,
sensor_status, ndvi_flag,
mill_id, crop_type, sowing_date, area_hectares,
has_metadata
```

---

## 3. Quick Analysis — NDVI Around Sowing

Filter: `sensor_status == OK` and `has_metadata == True`.  
Window: 30 days before sowing_date (exclusive) and 30 days after (inclusive).

```
 crop_type  mean_ndvi_before  mean_ndvi_after  n_parcels
   soybean            0.1706           0.3109          4
 sugarcane            0.1775           0.3362         19
     wheat            0.1761           0.3135          2
```

**Interpretation.** All three crops show roughly a doubling of NDVI from the pre-sowing window (~0.17) to the post-sowing window (~0.31–0.34). This is physically consistent: pre-sowing fields are bare soil or residue, while the post-sowing period captures germination and early canopy development. Sugarcane's marginally higher post-sowing value (0.336 vs. ~0.31 for the grain crops) plausibly reflects its faster early leaf-area expansion, though the wheat sample (n=2 parcels) is too small to draw confident conclusions. The uniformity of pre-sowing NDVI across all three crops suggests similar field-preparation practices across mills.

---

## 4. Production-Readiness Reflection

### Three things I'd change for a 100× larger daily pipeline

**1. Switch to a columnar format and lazy execution.**  
At 100× scale (~350k rows/day, growing to millions historically) CSV read/write becomes a bottleneck and pandas loads everything into memory eagerly. I'd switch storage to Parquet (partitioned by `date` and `parcel_id` prefix) and use Polars or DuckDB for the transformation step — both operate lazily and avoid materialising full tables unnecessarily. Incremental daily appends then touch only the day's partition, not the entire history.

**2. Add explicit schema validation at ingestion.**  
Right now the pipeline discovers problems after the fact. At scale, a bad upstream feed silently corrupting 10% of a day's data is far worse than a fast failure. I'd add a schema contract layer (Pandera or Great Expectations) that runs before any cleaning: assert column types, assert `ndvi_value` is float, assert `sensor_status` is not 100% null, assert date parsability rate > 99%. This makes the pipeline fail loudly and early rather than produce subtly wrong output.

**3. Parameterise and orchestrate, don't script.**  
A cron-run Python script has no retry logic, no dependency tracking, no visibility into partial failures. I'd move this into a DAG (Airflow, Prefect, or even a simple cloud workflow). The cleaning step, the join, and the analysis would be separate tasks with defined inputs/outputs, so a metadata-feed failure doesn't re-run the (expensive) readings cleaning step unnecessarily.

---

### What I'd monitor in production

- **Row count per parcel per day** — alert if any parcel drops to zero readings unexpectedly; could be sensor offline or feed failure.
- **NDVI null rate** (out-of-range + sensor errors combined) — a sudden spike indicates a sensor firmware issue or satellite coverage gap.
- **Date parsing failure rate** — if a new upstream feed introduces a fourth date format, this is the first signal.
- **Orphan parcel rate** — if readings arrive for parcel IDs not in metadata, it means a new parcel was commissioned but metadata wasn't updated; business-critical if it feeds a mill-level report.
- **Pipeline latency** — how long the daily run takes; a 2× slowdown before it becomes a problem is much easier to investigate than a 10× slowdown under pressure.

---

### Most likely thing to silently break

**The date parser will encounter a new format and produce NaT without failing.**

The current multi-format parser tries three known formats in sequence and returns `NaT` for anything that doesn't match. If a fourth feed is added with, say, `MM-DD-YYYY`, the parse will silently produce `NaT`, those rows will be dropped, and the only signal will be a marginally lower row count — which could easily be mistaken for a quiet day on the farm. Without a hard assertion that parse success rate stays above some threshold (e.g. 99%), this class of error is nearly invisible until someone notices that a whole parcel's history has gone missing.

The fix is one line in schema validation: `assert df['date'].isna().mean() < 0.01`.
