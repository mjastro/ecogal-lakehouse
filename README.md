# ecogal-lakehouse

A Databricks lakehouse pipeline combining an ALMA continuum source catalogue from ECOGAL (https://doi.org/10.5281/zenodo.17640218)
with the public DAWN JWST Archive (DJA) spectroscopic redshift catalogue (https://doi.org/10.5281/zenodo.15472354) —
built to test Databricks implementation and gain genuine, hands-on experience with production data engineering patterns beyond course-level familiarity: the medallion architecture, watermark-driven incremental ingestion of an irregularly-updated external
source, spatial cross-matching at scale, auditable data quality gates, and
orchestrated, scheduled execution.


## Architecture — medallion (bronze → silver → gold)

```
ecogal_prior_all_v1.csv         DJA catalogue (public S3, versioned)
        │                              │
        ▼                              ▼  (watermark-checked: only
┌───────────────┐              ┌────────────────┐   re-ingested if a newer
│ bronze.        │              │ bronze.         │   version is available)
│ sources_raw    │              │ jwst_spectra_raw│
└───────┬────────┘              └────────┬────────┘
        ▼                                 ▼
┌────────────────────┐          ┌──────────────────────────┐
│ silver.             │          │ silver.                   │
│ sources_validated   │          │ jwst_spectra_validated     │
│ (schema enforcement, │          │ (grade-3 filter,           │
│  quality flags,      │          │  one primary row/source)   │
│  quality gate)       │          └───────────┬────────────────┘
└──────────┬───────────┘                      │
           │         spatial cross-match       │
           │      (astropy, 0.5" tolerance)     │
           └────────────────┬───────────────────┘
                             ▼
                 ┌────────────────────────────┐
                 │ silver.                     │
                 │ jwst_alma_crossmatch         │
                 └───────────┬──────────────────┘
                              ▼
                 ┌─────────────────────────────────────┐
                 │ gold.alma_sources_with_specz          │
                 │ (band dimension join +                │
                 │  evolving best-redshift +             │
                 │  version-based change detection)      │
                 └────────────────────────────────────────┘
```

## Bring your own data

The source catalogues are not included in this repo. The ALMA catalogue from ECOGAL is available here:
https://doi.org/10.5281/zenodo.17640218; 
; the DJA spectroscopic catalogue is public and
available at **https://dawn-cph.github.io/dja/index.html**. Update the
placeholder paths in `notebooks/01_bronze_ingest.py` to point at your own uploaded files (Databricks
Unity Catalog Volumes recommended over Workspace Files for anything beyond a
personal/practice project).

## Key design decisions

**Watermark-driven ingestion, no stored credentials.** The DJA catalogue
updates irregularly. Rather than storing personal AWS credentials in a job
that could run unattended, version availability is checked via lightweight,
unauthenticated HTTP HEAD requests against candidate version URLs. A
watermark table (`control.ingestion_watermark`) records the last ingested
version, so the pipeline is a fast, cheap no-op when nothing has changed.

**Flag, don't silently drop.** Data quality checks in the silver layer add
boolean flag columns rather than deleting rows outright — failures stay
visible and auditable. The one deliberate exception is the JWST redshift
grade filter, which is a genuine hard requirement for this specific use
case (only grade-3/secure redshifts are fit to update the catalogue), and is
documented as such in the notebook.

**Multi-observation sources are a feature, not a bug.** ALMA sources are
often observed multiple times, at different bands/depths, by independent
programmes. Rather than deduplicating these away, the pipeline tracks
`bands_observed` per source explicitly.

**Spatial cross-matching against a shared reference, not directly
peer-to-peer.** JWST sources are matched against the *same prior reference
positions* the ALMA catalogue itself was already matched to, rather than
matching JWST directly to ALMA — avoiding compounding position uncertainty
from two independent, lower-resolution catalogues. Done with
`astropy.coordinates` on the driver (tens of thousands of rows per side —
well within comfortable memory limits), validated for consistency against
an independent TOPCAT match.

**Evolving data, with the original preserved.** The gold table's
`zsp_best_avail_updated` column prefers a new JWST measurement when
available; the original `zsp_best_avail` value from data release is kept
unchanged alongside it, for reproducibility. `has_jwst_filled_gap`
specifically distinguishes "JWST filled a source that previously had no
valid redshift" from "JWST re-measured a source that already had one" —
a common source of inflated "what's new" counts if not handled explicitly.

**Change detection via Delta versioning, not a separate tracker.** Because
an `overwrite` write to a Delta table creates a new version rather than
destroying history, "what's new since last run" is answered by comparing
the current output against the *previous version of the same gold table*
directly (`DESCRIBE HISTORY` + `versionAsOf`), rather than maintaining a
separate change-log table.

## Orchestration

Scheduled as a Databricks Job running the three notebooks in strict
sequence, each task depending on the previous one succeeding:

![Job DAG](docs/job_dag_screenshot.png)

## What this project demonstrates

| Skill area | Where |
|---|---|
| ETL / dimensional data modelling | `01_bronze_ingest.py`, `03_gold_enrich.py` (fact/dimension join) |
| Lakehouse architecture (Delta Lake, medallion pattern) | All three notebooks |
| Data quality (flag-based, auditable) | `02_silver_validate_crossmatch.py` |
| Orchestration | Databricks Jobs (screenshot above) |
| Observability / reliable operations | Quality gates; watermark-based incremental ingestion avoids wasted reprocessing |
| Lineage | Lineage notes at the end of each notebook; `dja_updated_version` traces each updated value to its source catalogue version |
| Integration of diverse data sources | Spatial cross-match with an entirely separate public catalogue (DJA) |
| Data versioning / change detection | `03_gold_enrich.py`, using Delta Lake time travel rather than a bespoke tracking table |

## Status

Working end-to-end pipeline, orchestrated and scheduled. Built iteratively,
including working through real design questions along the way (PySpark
window functions vs. Python conditionals, driver-side vs. distributed
spatial joins, medallion layer boundaries, watermark design without stored
credentials).
