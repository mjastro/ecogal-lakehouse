# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "astropy",
# ]
# ///
# MAGIC %md
# MAGIC # Silver Layer: Schema Enforcement + Data Quality Validation
# MAGIC Takes the raw bronze table, enforces a strict schema, and applies
# MAGIC data quality checks. Rows that fail validation are flagged, not silently
# MAGIC dropped, so failures remain auditable.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructType, StructField, BooleanType, IntegerType

spark = SparkSession.builder.getOrCreate()


BRONZE_TABLE = "alma_lakehouse.bronze.sources_raw_prior"
SILVER_TABLE = "alma_lakehouse.silver.sources_validated_prior"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load bronze

# COMMAND ----------

df = spark.table(BRONZE_TABLE)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema enforcement
# MAGIC Cast measurement/coordinate columns to explicit numeric types.
# MAGIC Adjust column names below to match your actual CSV headers exactly.

# COMMAND ----------

df_typed = (
    df
    .withColumn("RA_peak_alma", F.col("RA_peak_alma").cast(DoubleType()))
    .withColumn("DEC_peak_alma", F.col("DEC_peak_alma").cast(DoubleType()))
    .withColumn("frequency", F.col("frequency").cast(DoubleType()))
    .withColumn("beam_maj", F.col("beam_maj").cast(DoubleType()))
    .withColumn("separation_prior", F.col("separation_prior").cast(DoubleType()))
    .withColumn("flux_peak", F.col("flux_peak").cast(DoubleType()))
    .withColumn("noise", F.col("noise").cast(DoubleType()))
    .withColumn("sn", F.col("sn").cast(DoubleType()))
    .withColumn("flux_aper", F.col("flux_aper").cast(DoubleType()))
    .withColumn("eflux_aper", F.col("eflux_aper").cast(DoubleType()))
    .withColumn("z_phot_eazy", F.col("z_phot_eazy").cast(DoubleType()))
    .withColumn("zsp_best_avail", F.col("zsp_best_avail").cast(DoubleType()))
    .withColumn("band", F.col("band").cast(StringType()))
    .withColumn("projectID", F.col("projectID").cast(StringType()))
    .withColumn("srcid", F.col("srcid").cast(DoubleType()))
    .withColumn("id_new", F.col("id_new").cast(StringType()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality checks - correct source identification catalogue 
# MAGIC Each check adds a boolean flag column rather than dropping rows outright —
# MAGIC this keeps the validation auditable and lets downstream users decide
# MAGIC how strict to be, rather than silently losing data.

# COMMAND ----------

df_checked = (
    df_typed
    .withColumn(
        "qc_valid_coords",
        (F.col("RA_peak_alma").between(0, 360)) & (F.col("DEC_peak_alma").between(-90, 90))
    )
    .withColumn(
        "qc_positive_noise",
        F.col("noise") > 0
    )
    .withColumn(
        "qc_sn_consistent",
        # sn should roughly equal peak_flux / noise; flag large discrepancies for 'detection'
        F.when(
            F.col("flux_peak")>0,
            F.abs((F.col("flux_peak") / F.col("noise")) - F.col("sn")) < 0.5
        ).otherwise(
            F.col("noise")>0
        )
    )
    .withColumn(
        "qc_separation",
        # taking into account the separation and teh beam size (especially in case of detection)
        F.when(
            F.col("beam_maj")>0.25,
            F.col("separation_prior") < F.col("beam_maj") * 0.6
        ).otherwise(
            F.col("separation_prior")<0.2
        )
    )
    .withColumn(
        "qc_all_pass",
        F.col("qc_valid_coords") & F.col("qc_positive_noise") & F.col("qc_sn_consistent") & F.col("qc_separation")
    )
    .withColumn(
        "qc_eazypy_pass",
        F.col('z_phot_eazy')>0
    )
    .withColumn(
        "qc_zspec",
        F.col('zsp_best_avail')>0
    )
)

n_total = df_checked.count()
n_pass = df_checked.filter(F.col("qc_all_pass")).count()
n_fail = n_total - n_pass

print(f"Total rows: {n_total}")
print(f"Passed all quality checks: {n_pass} ({100*n_pass/n_total:.1f}%)")
print(f"Failed at least one check: {n_fail} ({100*n_fail/n_total:.1f}%)")

# Break down failures by check, to see which check is driving failures
display(
    df_checked.select(
        F.sum(F.when(~F.col("qc_valid_coords"), 1).otherwise(0)).alias("fail_coords"),
        F.sum(F.when(~F.col("qc_positive_noise"), 1).otherwise(0)).alias("fail_noise"),
        F.sum(F.when(~F.col("qc_sn_consistent"), 1).otherwise(0)).alias("fail_sn_consistency"),
        F.sum(F.when(~F.col("qc_separation"), 1).otherwise(0)).alias("fail_separation"),
        F.sum(F.when(~F.col("qc_eazypy_pass"), 1).otherwise(0)).alias("fail_eazypy"),
        F.sum(F.when(~F.col("qc_zspec"), 1).otherwise(0)).alias("fail_zspec"),        
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication check
# MAGIC There are programs with multiple observaations for the same target in different bands, depths, and resolutions. Worth adding a flag for this.

# COMMAND ----------

from pyspark.sql.window import Window

observation_window = Window.partitionBy("id_new")

df_final = (
    df_checked
    .withColumn("n_observations", F.count("*").over(observation_window))
    .withColumn("has_multiple_observations", F.col("n_observations") > 1)
    .withColumn("bands_observed", F.collect_set("band").over(observation_window))
)

n_multi_sources = (
    df_final.filter(F.col("has_multiple_observations"))
    .select("id_new").distinct().count()
)
print(f"Distinct sources with multiple observations (across bands/programs): {n_multi_sources}")

# COMMAND ----------

display(df_final.limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta (silver)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS alma_lakehouse.silver")

(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Silver table written: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality gate — fail the job if quality has degraded significantly
# MAGIC This is what turns "we compute quality metrics" into "we act on them" —
# MAGIC the observability/reliable-operations piece. Threshold is illustrative;
# MAGIC adjust based on what's actually normal for this dataset.

# COMMAND ----------

QC_PASS_THRESHOLD = 0.90  # require at least 90% of rows to pass all checks

pass_rate = n_pass / n_total
if pass_rate < QC_PASS_THRESHOLD:
    raise ValueError(
        f"Data quality gate FAILED: only {pass_rate:.1%} of rows passed validation "
        f"(threshold: {QC_PASS_THRESHOLD:.0%}). Halting pipeline before gold layer."
    )
else:
    print(f"Quality gate PASSED: {pass_rate:.1%} of rows valid.")

# COMMAND ----------

df_final.describe()

# COMMAND ----------

bronze_count = spark.table("alma_lakehouse.bronze.sources_raw_prior").count()
silver_count = spark.table("alma_lakehouse.silver.sources_validated_prior").count()

print(f"Bronze rows: {bronze_count}")
print(f"Silver rows: {silver_count}")
print(f"Match: {bronze_count == silver_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checking the delta log
# MAGIC * Since extra column for eazypy + spec-z quality flags were added, and overwritten, there are two versions in the silver delta lake.

# COMMAND ----------

spark.sql("DESCRIBE HISTORY alma_lakehouse.bronze.sources_raw_prior").display()

# COMMAND ----------

spark.sql("DESCRIBE HISTORY alma_lakehouse.silver.sources_validated_prior").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare JWST graded catalogue in the silver layer, remove duplicate and ALMA source match

# COMMAND ----------

# MAGIC %pip install astropy

# COMMAND ----------

from astropy.coordinates import SkyCoord
import astropy.units as u
import pandas as pd
from pyspark.sql import functions as F


# COMMAND ----------

#filter out the grade=3 version first
JWST_BRONZE_TABLE = "alma_lakehouse.bronze.jwst_spectra_raw"
jwst_df_raw = spark.table(JWST_BRONZE_TABLE)

# COMMAND ----------

display(jwst_df_raw.limit(10))

# COMMAND ----------

jwst_df_raw_typed = (
    jwst_df_raw
    .withColumn("objid", F.col("objid").cast(DoubleType()))
    .withColumn("ra", F.col("ra").cast(DoubleType()))
    .withColumn("dec", F.col("dec").cast(DoubleType()))
    .withColumn("file", F.col("file").cast(StringType()))
    .withColumn("root", F.col("root").cast(StringType()))
    .withColumn("valid", F.col("valid").cast(BooleanType()))
    .withColumn("grade", F.col("grade").cast(IntegerType()))
    .withColumn("zgrade", F.col("zgrade").cast(DoubleType()))
)

# COMMAND ----------

display(jwst_df_raw_typed.limit(10))

# COMMAND ----------

GRADE_THRESHOLD = 3

jwst_df_quality_filtered = (
    jwst_df_raw_typed
    .filter(F.col("grade") == GRADE_THRESHOLD)
)

print(f"JWST rows with secure (grade {GRADE_THRESHOLD}) redshifts: "
      f"{jwst_df_quality_filtered.count()} / {jwst_df_raw_typed.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## getting unique sources exluding duplication

# COMMAND ----------

from pyspark.sql.window import Window

objid_window = Window.partitionBy("objid")

jwst_df_flagged = jwst_df_quality_filtered.withColumn(
    "n_rows_per_objid", F.count("*").over(objid_window)
).withColumn(
    "has_multiple_rows", F.col("n_rows_per_objid") > 1
)

n_ambiguous_sources = (
    jwst_df_flagged.filter(F.col("has_multiple_rows"))
    .select("objid").distinct().count()
)
print(f"Distinct objid values with more than one grade-3 row: {n_ambiguous_sources}")

# COMMAND ----------

primary_window = Window.partitionBy("objid").orderBy(F.col("effexptm").desc()) #order by the effective exposure time

jwst_df_ranked = jwst_df_flagged.withColumn(
    "row_rank_within_objid", F.row_number().over(primary_window)
)

# The primary row per source — what you'd carry forward as "the" redshift
jwst_df_primary = jwst_df_ranked.filter(F.col("row_rank_within_objid") == 1)

# The full set stays available too, for anyone who wants to inspect
# the non-primary rows for a given objid
n_primary = jwst_df_primary.count()
n_total = jwst_df_ranked.count()
print(f"Primary rows selected: {n_primary} (from {n_total} total grade-3 rows)")

# COMMAND ----------

display(jwst_df_ranked.limit(10))

# COMMAND ----------

display(jwst_df_primary.limit(30))

# COMMAND ----------

(
    jwst_df_primary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("alma_lakehouse.silver.jwst_spectra_validated")
)

print("JWST silver table written: alma_lakehouse.silver.jwst_spectra_validated")

# COMMAND ----------

#getting unique ALMA prior position (for now it is using the ALMA catalogue - but later it'll be good to substitute with the original prior catalogue
# One row per unique source, before feeding into the coordinate match
alma_prior_unique = (
    spark.table("alma_lakehouse.silver.sources_validated_prior")
    .select("id_new", "RA_parent", "DEC_parent")
    .dropDuplicates(["id_new"])  # keeps one arbitrary row per id_new
)

n_before = spark.table("alma_lakehouse.silver.sources_validated_prior").select("id_new").count()
n_after = alma_prior_unique.count()
print(f"ALMA rows before dedup: {n_before}, unique sources: {n_after}")

alma_pdf = alma_prior_unique.toPandas()

# COMMAND ----------


# --- Pull the two sides to the driver as pandas ---

jwst_pdf = (
    spark.table("alma_lakehouse.silver.jwst_spectra_validated")
    .select("objid", "ra", "dec","catalogue_version","root","file","zgrade")
    .toPandas()
)

# --- Build SkyCoord objects ---
alma_coords = SkyCoord(ra=alma_pdf["RA_parent"].values * u.deg,
                        dec=alma_pdf["DEC_parent"].values * u.deg)
jwst_coords = SkyCoord(ra=jwst_pdf["ra"].values * u.deg,
                        dec=jwst_pdf["dec"].values * u.deg)

# --- Nearest-neighbour match: for each JWST source, find its closest ALMA-prior position ---
idx, sep2d, _ = jwst_coords.match_to_catalog_sky(alma_coords)

jwst_pdf["id_new"] = alma_pdf["id_new"].values[idx]
jwst_pdf["separation_arcsec"] = sep2d.arcsec


# --- Apply the 0.5 arcsec threshold ---
# match_to_catalog_sky always returns the nearest neighbour regardless of
# distance, so genuine non-matches must be filtered out explicitly here.
SEPARATION_THRESHOLD_ARCSEC = 0.5
jwst_pdf["is_matched"] = jwst_pdf["separation_arcsec"] < SEPARATION_THRESHOLD_ARCSEC

n_matched = jwst_pdf["is_matched"].sum()
print(f"JWST sources matched within {SEPARATION_THRESHOLD_ARCSEC} arcsec: "
      f"{n_matched} / {len(jwst_pdf)} ({100*n_matched/len(jwst_pdf):.1f}%)")


# COMMAND ----------

jwst_pdf[:10]

# COMMAND ----------


# --- Push the match table back to Spark and write it ---
df_crossmatch = spark.createDataFrame(jwst_pdf)

(
    df_crossmatch.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("alma_lakehouse.silver.jwst_almaprior_crossmatch")
)

print("Cross-match table written: alma_lakehouse.silver.jwst_almaprior_crossmatch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage note
# MAGIC ### ALMA
# MAGIC Source: `alma_lakehouse.bronze.sources_raw`
# MAGIC Target: `alma_lakehouse.silver.sources_validated`
# MAGIC Transformation: schema enforcement (explicit types), quality flags
# MAGIC (coordinate validity, positive noise, S/N consistency, prior separation to ALMA peak),
# MAGIC quality gate (blocks pipeline if pass rate < 90%)
# MAGIC
# MAGIC ### JWST (DJA)
# MAGIC Source: `alma_lakehouse.bronze.jwst_spectra_raw`
# MAGIC Target: `alma_lakehouse.silver.jwst_almaprior_crossmatch`
# MAGIC Transformation: schema enforcement (explicit types), quality flags (grade==3), duplication rejection, ALMA source crossmatch (<0.5 arcsec)