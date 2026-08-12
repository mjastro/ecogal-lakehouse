# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold Layer: Enrichment + DJA up-to-date spec-z demension join
# MAGIC Joins the validated silver source catalogue (the "fact" table) against
# MAGIC a spectroscopic redshift table from the latest DJA spectra (a "dimension"
# MAGIC table) - an example of integrating a second, independently-sourced dataset rather than
# MAGIC working from one file alone.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------

#Delete schema just for the refreshment
#spark.sql("DROP SCHEMA IF EXISTS alma_lakehouse.gold CASCADE")

# COMMAND ----------

# Add Gold Schema
spark.sql("CREATE SCHEMA IF NOT EXISTS alma_lakehouse.gold")

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN alma_lakehouse").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the dimension table and join (silver layer DJA crossmatched)
# MAGIC This is a small, independently-maintained reference dataset —
# MAGIC not derived from the source catalogue itself.

# COMMAND ----------

target_table = "alma_lakehouse.gold.alma_sources_prior_with_updated_specz"

alma_df = spark.table("alma_lakehouse.silver.sources_validated_prior")
df_jwst_matched = spark.table("alma_lakehouse.silver.jwst_almaprior_crossmatch")

jwst_side = (
    df_jwst_matched
    .filter(F.col("is_matched"))
    .select("id_new", "zgrade", "separation_arcsec", "catalogue_version", "root", "file")
    .withColumnRenamed("zgrade", "zsp_dja_new")
    .withColumnRenamed("catalogue_version", "dja_updated_version")
)

alma_enriched = (
    alma_df.alias("alma")
    .join(jwst_side.alias("jwst"), on="id_new", how="left")
    .select(
        "alma.*",
        F.col("jwst.zsp_dja_new").alias("zsp_dja_new"),
        F.coalesce(F.col("jwst.zsp_dja_new"), F.col("alma.zsp_best_avail")).alias("zsp_best_avail_updated"),
        (
            F.col("jwst.zsp_dja_new").isNotNull() & (F.col("alma.zsp_best_avail") < 0) & (F.col("jwst.zsp_dja_new")>0)
        ).alias("has_jwst_filled_gap"),
        F.col("jwst.dja_updated_version").alias("dja_updated_version"),
        F.coalesce(F.col("jwst.file"), F.col("alma.file")).alias("file_new"),
        F.col("jwst.root").alias("root")
    )
    .drop("file")
    .withColumnRenamed("file_new", "file")
)


# COMMAND ----------

display(alma_enriched.limit(10))

# COMMAND ----------

display(alma_enriched.filter(F.col("has_jwst_filled_gap")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to delta (Gold)

# COMMAND ----------


# --- Detect newly-added spec-z, comparing against the previous version of this table ---
try:
    latest_version = (
        spark.sql(f"DESCRIBE HISTORY {target_table}")
        .agg(F.max("version"))
        .collect()[0][0]
    )
    previous_specz_ids = set(
        row["id_new"] for row in
        spark.read.format("delta")
        .option("versionAsOf", latest_version)
        .table(target_table)
        .filter(F.col("has_jwst_filled_gap"))
        .select("id_new").distinct().collect()
    )
except Exception:
    previous_specz_ids = set()  # first run — table doesn't exist yet
    print("First run")

alma_enriched = alma_enriched.withColumn(
    "is_newly_added_specz",
    F.col("has_jwst_filled_gap") & (~F.col("id_new").isin(previous_specz_ids))
)

n_new = alma_enriched.filter(F.col("is_newly_added_specz")).select("id_new").distinct().count()
n_total_previous = alma_enriched.filter(F.col("zsp_best_avail") > 0).select("id_new").distinct().count()
n_total_jwst_specz = alma_enriched.filter(F.col("zsp_best_avail_updated") > 0).select("id_new").distinct().count()

print(f"Sources with a JWST spec-z: {n_total_jwst_specz} total, {n_new} newly added this run ({n_total_previous} in the previous version)")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Save golden layer

# COMMAND ----------


(
    alma_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)
print(f"Written: {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary view (a small, genuinely useful analytical output)

# COMMAND ----------

display(
    alma_enriched.groupBy("id_new")
    .agg(
        F.count("*").alias("has_jwst_filled_gap")
    )
    .orderBy("id_new")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage note
# MAGIC Sources: `alma_lakehouse.silver.sources_validated` 
# MAGIC          + `alma_lakehouse.silver.jwst_almaprior_crossmatch` (independent reference dataset)
# MAGIC Target: `alma_lakehouse.gold.alma_sources_prior_with_updated_specz`
# MAGIC Transformation: dimensional join (fact + dimension)