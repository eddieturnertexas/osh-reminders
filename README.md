```
   ___  ____  _   _
  / _ \/ ___|| | | |      OAK STREET HEALTH
 | | | \___ \| |_| |      Appointment Reminder Pipeline
 | |_| |___) |  _  |      Databricks Asset Bundle / Medallion
  \___/|____/|_| |_|
```

# OSH Appointment Reminder Pilot

A Databricks Asset Bundle that pulls the patient list from an API, refines it through a
bronze/silver/gold medallion, and produces a ranked, week-by-week send plan for a
1,000-patient SMS reminder campaign.

**Author:** eddie turner · **Date:** 2026-09-14

---

## The brief

OSH is piloting a text-message appointment reminder campaign. The campaign texts 1,000
High (Serious) and Highest (VIP) risk patients to schedule an appointment with their
primary care physician. To keep call-center traffic manageable, a 20% threshold applies
over the course of 5 weeks. After the 5-week threshold, higher-risk patients take
priority for reminders.

Working assumptions from the brief:

- A POST API call supplies the patient list.
- The initial list is predefined but grows as new VIP patients appear.
- Priority logic is needed, driven by risk factors.
- Reporting is needed on scheduling outcomes.

---

## Solution at a glance

```
  OSH API  ──POST /payload?limit=200&offset=N──▶  BRONZE        raw VARIANT payload
  (FastAPI on Azure App Service)                 osh_event      2,000 rows
                                                     │
                                                     ▼
                                                  SILVER        typed + normalised
                                                  dim_osh_event 1,800 rows
                                                     │              (consent filter)
                                                     ▼
                                    ┌────────────────┴────────────────┐
                                    ▼                                 ▼
                              GOLD (fact)                       GOLD (plan)
                              fact_osh_reminder                 fact_osh_campaign_plan
                              1,800 rows                        1,000 rows / 5 weeks
```

Three Unity Catalog catalogs — `dev_osh_bronze_db`, `dev_osh_silver_db`,
`dev_osh_gold_db` — deployed as one bundle across dev/qa/prod targets via a
`catalog_env` prefix. All compute is serverless.

---

## How each requirement is met

### "A POST API call will be made with the list of patients"

`bronze_ingestion` reads the API address and bearer token from Databricks secrets
(pointers stored per-stream in the config table, so credentials never enter the repo),
then pages the endpoint 200 records at a time until it reaches 2,000 or the API returns
a short page. Each page is committed to Delta on its own, so a failure at batch 7 keeps
batches 1–6, and the merge on a content-hash `Id` makes re-running any page a no-op.

The whole API record lands in a single `VARIANT` column alongside `Id` and
`bronze_layer_timestamp`. No payload field is named in bronze, so a new or renamed API
field needs no schema migration — field extraction is silver's job.

### "Priority logic will be needed based on certain risk factors"

`gold_osh_campaign` ranks the eligible cohort by, in order:

1. **Risk** — Highest/VIP, then High/Serious, then Medium/Moderate
2. **Need** — no appointment first, then cancelled
3. **Newness** — `new_vip` ahead of `initial` within a risk tier
4. **Overdue** — longest time since last PCP visit
5. `Id`, as a deterministic tiebreak

### "A 20% threshold over 5 weeks"

20% of the 1,000 cohort is 200 patients per week, and the plan assigns
`send_campaign_week` 1–5 by rank, 200 to each. Week 1 starts at the **current week of
year** and the following four run consecutively. The week columns are derived by date
arithmetic (`date_add`, then `weekofyear`) rather than by adding to a week number, so
they stay correct across a year boundary instead of producing week 53, 54, 55.

The cap is **per week and resets**, not a cumulative 200 — so the campaign delivers
200/400/600/800/1000, rather than exhausting its budget in week 1.

### "The list can grow based on new VIP patients"

The plan is rebuilt from the current cohort on every run, so a newly arrived VIP enters
the ranking immediately and outranks lower-risk patients who have not yet been texted.
`new_vip` is also an explicit tiebreak above `initial` within the same risk tier.

### "Reporting will be needed based on scheduling outcomes"

`fact_osh_reminder` is the reportable fact: one row per event at the silver grain,
carrying `patient_key`, `location_key` and calendar attributes from the three
dimensions, plus derived measures — `is_reminder_eligible`,
`days_since_last_pcp_visit`, `web_submission_lag_seconds`, `approval_lag_seconds`.
Every pipeline run also writes to `osh_config_log_header` and
`osh_config_log_header_detail`, giving a per-batch ledger of what was ingested when.

---

## The cohort maths

The brief asks for 1,000 patients. The High+Highest pool does not contain 1,000 that can
actually be texted, and the pipeline surfaces that rather than quietly under-delivering:

| tier | patients | running |
|---|---:|---:|
| Highest/VIP, no appointment | 382 | 382 |
| High/Serious, no appointment | 393 | 775 |
| High+Highest, cancelled | 58 | 833 |
| Medium/Moderate, no appointment | 167 | **1,000** |

Of 2,000 patients, 1,200 are High or Highest; 1,077 of those consent to SMS; 775 still
have no appointment. Two deliberate judgment calls close the gap to 1,000:

- **`cancelled` counts as needing an appointment.** A cancelled appointment still means
  the patient has no appointment. Worth 58 patients.
- **The last 167 come from the next risk tier**, labelled `T3 top-up - next risk tier`
  so reporting can measure core-cohort response separately. Drop the Medium tier and
  the pilot is 833 patients — a defensible alternative, but it should be a stated
  decision rather than a silent shortfall.

The result is visible in the plan: weeks 1–2 are entirely Highest/VIP, weeks 3–4 are
almost entirely High/Serious, and the Medium top-up lands in week 5 — lowest priority
contacted last, exactly as the brief requires.

---

## Design decisions worth defending

**Bronze stores VARIANT, not columns.** Schema-on-read at the raw layer means an API
change never breaks ingestion. Silver owns the contract.

**`Id` is a content hash (`xxhash64`), not a sequence.** `monotonically_increasing_id()`
restarts per DataFrame, so it collides across runs and cannot be a primary key. A hash
of the canonical JSON is stable across runs, which is what makes the merge idempotent
and re-running a page safe.

**The campaign plan is rebuilt, not merged.** Ranking is relative: one patient booking
an appointment shifts everyone behind them. A merge would leave stale ranks and grow
past the 1,000 rows the plan is meant to hold. `fact_osh_reminder` is untouched by this
and remains the incremental fact.

**Backfill needs no mechanism.** Eligibility is re-evaluated from scratch on every run
and ranks are assigned over the survivors, so anyone who opted out or self-scheduled
since the last dispatch simply is not there and the ranks close up behind them.

**Orchestration is config-driven.** `<layer>_ingestion` reads
`osh_config_<layer>_stream`, walks the enabled streams in `RunOrder`, and dispatches to
whatever notebook the config row names. Adding `fact_osh_campaign_plan` required a
config row and a notebook — no new job task.

**Dimensions sit in silver, the star schema is gold.** `dim_patients`, `dim_location`
and `dim_date` are conformed in silver; gold joins them into the fact. The honest
caveat is that gold does not yet re-expose the dimensions, which would make the layering
cleaner.

---

## Known gaps

Stated deliberately, because the brief says to expect the unexpected:

- **Silver and gold reads are not incremental.** Writes are idempotent, but each run
  rescans its whole source. At 2,000 rows this is noise; past a few million it needs a
  watermark on `bronze_layer_timestamp`.
- **Consent revocation does not propagate.** Silver filters `sms_consent = true` on the
  way in, but the merge has no `WHEN NOT MATCHED BY SOURCE THEN DELETE`, so a patient
  who revokes consent keeps their existing silver and gold row. A suppression list
  checked at send time is the correct guard; the stale row is still a gap.
- **The send log and suppression tables are designed, not built.** Tracking which
  patients were actually texted, and their outcomes, is what turns the plan into
  closed-loop reporting.

---

## Repository layout

```
databricks.yml                     bundle root: name, host, includes, dev/qa/prod targets
config/env_variables.yml           every tunable: catalogs, schemas, batch size, perf target
resources/
  bootstrap_init.job.yml           creates and seeds all objects
  osh_ingestion.job.yml            bronze -> silver -> gold, six serverless tasks
notebooks/
  bootstrap/                       catalogs, schemas, volume, config tables, seed dimensions
  bronze_notebooks/                status check + paged API ingestion
  silver_notebooks/                status check + orchestrator + typed transform
  gold_notebooks/                  status check + orchestrator + fact + campaign plan
API_simulator/main.py              stands in for the OSH source API
```

## Running it

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run bootstrap_init -t dev     # catalogs, schemas, config, dimensions
databricks bundle run osh_ingestion  -t dev     # bronze -> silver -> gold
```

`bootstrap_init` is re-runnable — every object is created `IF NOT EXISTS`. For a clean
reload, drop the three data tables and leave the catalogs in place; recreating catalogs
is a slow Unity Catalog control-plane operation and re-running bootstrap is then
unnecessary, since the config tables survive.

## Measured

| | |
|---|---|
| API pull | 2,000 records, 10 pages of 200, ~0.25s per page |
| Payload compression | 898 KB raw → 115 KB gzipped (7.8x) |
| bronze / silver / gold | 2,000 / 1,800 / 1,800 |
| campaign plan | 1,000 rows, 200 per week, weeks 38–42 |
| bootstrap run | 93s |
| full ingestion run | 305s |

Serverless `performance_target` is the single biggest lever on run time: startup was
measured at 255s under `STANDARD` against 5s under `PERFORMANCE_OPTIMIZED` in the same
workspace. Dev uses the fast setting; a nightly production batch that nobody waits on
should use `STANDARD` and save the cost.
