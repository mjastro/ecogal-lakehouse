# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "astropy",
# ]
# ///
# MAGIC %md
# MAGIC # Bronze Layer: Raw Ingestion
# MAGIC Ingests the raw ALMA source catalogue CSV as-is into a Delta table,
# MAGIC with no transformation — preserving the original data exactly as received.
# MAGIC This is the "bronze" layer of the medallion architecture.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Update this path to wherever you upload the CSV in your Databricks workspace
# MAGIC (e.g. via Catalog > + Add data > Upload files, or DBFS).

# COMMAND ----------

RAW_CSV_PATH = "YOUR_PAHT_TO_CATALOGUE/ecogal_prior_all_v1.csv"  # <--- change it for your catalogue
BRONZE_TABLE = "alma_lakehouse.bronze.sources_raw_prior"

# COMMAND ----------

# MAGIC %md
# MAGIC deleting the lakehouse file due to the update of the catalogue naming

# COMMAND ----------

spark.sql("DROP CATALOG alma_lakehouse CASCADE")

# COMMAND ----------

spark.sql("SHOW CATALOGS").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read raw CSV
# MAGIC Read permissively at this stage — we do NOT enforce schema here.
# MAGIC Bronze should be a faithful, lossless copy of the source data.

# COMMAND ----------

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_CSV_PATH)
)

print(f"Rows ingested: {df_raw.count()}")
df_raw.printSchema()
display(df_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta (bronze)
# MAGIC Create the catalog/schema first if they don't exist, then write.

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS alma_lakehouse")
spark.sql("CREATE SCHEMA IF NOT EXISTS alma_lakehouse.bronze")

(
    df_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

print(f"Bronze table written: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Adding DJA spectra component in the bronze layer- to update the cross-matched sample (for later purpose)
# MAGIC

# COMMAND ----------

# Create the control schema
spark.sql("CREATE SCHEMA IF NOT EXISTS alma_lakehouse.control")

# Create the watermark table with a defined schema
spark.sql("""
    CREATE TABLE IF NOT EXISTS alma_lakehouse.control.ingestion_watermark (
        source_name STRING,
        version STRING,
        last_checked_at TIMESTAMP,
        last_ingested_at TIMESTAMP
    )
    USING DELTA
""")

# Initialize a starting row for JWST — only needed once, on first setup.
# Use a deliberately "low" starting version so the very first real check
# finds something newer and ingests it properly.
spark.sql("""
    INSERT INTO alma_lakehouse.control.ingestion_watermark
    VALUES ('jwst_spectra', 'v4.4', current_timestamp(), NULL)
""")

# COMMAND ----------

import requests

def version_exists(catalogue_name, version, url_prefix):
    """Cheap existence check — HEAD request, no download."""
    url = f"{url_prefix}/{catalogue_name}_{version}.csv.gz"
    try:
        resp = requests.head(url, timeout=10)
        return resp.status_code == 200
    except requests.RequestException:
        return False


def find_latest_version(catalogue_name, url_prefix, last_known_version,
                          max_minor_probes=10, max_major_probes=3):
    """
    Assumes a 'major.minor' versioning scheme (e.g. v4.5).
    Probes forward from the last known version to find the latest
    available, without assuming we know the true latest in advance.
    """
    major, minor = last_known_version.lstrip("v").split(".")
    major, minor = int(major), int(minor)

    latest_found = f"v{major}.{minor}"

    # Try incrementing the minor version first
    for m in range(minor + 1, minor + 1 + max_minor_probes):
        candidate = f"v{major}.{m}"
        if version_exists(catalogue_name, candidate, url_prefix):
            latest_found = candidate
        else:
            break  # assume monotonic, no gaps — stop at first miss

    # If minor probing found nothing new, try bumping the major version
    if latest_found == last_known_version:
        for maj in range(major + 1, major + 1 + max_major_probes):
            candidate = f"v{maj}.0"
            if version_exists(catalogue_name, candidate, url_prefix):
                latest_found = candidate
                # then probe forward on minor within this new major
                for m in range(1, max_minor_probes):
                    next_candidate = f"v{maj}.{m}"
                    if version_exists(catalogue_name, next_candidate, url_prefix):
                        latest_found = next_candidate
                    else:
                        break
                break

    return latest_found

# COMMAND ----------

from pyspark.sql import functions as F

URL_PREFIX = "https://s3.amazonaws.com/msaexp-nirspec/extractions"
catalogue_name = f'dja_msaexp_emission_lines'
#last_ingested_version = f'v4.4'

last_ingested_version = (
    spark.table("alma_lakehouse.control.ingestion_watermark")
    .filter(F.col("source_name") == "jwst_spectra")
    .collect()[0]["version"]
)

# COMMAND ----------

last_ingested_version

# COMMAND ----------


latest_available = find_latest_version(
    catalogue_name=catalogue_name,
    url_prefix=URL_PREFIX,
    last_known_version=last_ingested_version,
)

# COMMAND ----------

latest_available

# COMMAND ----------


if latest_available != last_ingested_version:
    # ... download and ingest ...
    spark.sql(f"""
        UPDATE alma_lakehouse.control.ingestion_watermark
        SET version = '{latest_available}',
            last_checked_at = current_timestamp(),
            last_ingested_at = current_timestamp()
        WHERE source_name = 'jwst_spectra'
    """)
    print(f"Updating the watermark to {latest_available}")
else:
    spark.sql(f"""
        UPDATE alma_lakehouse.control.ingestion_watermark
        SET last_checked_at = current_timestamp()
        WHERE source_name = 'jwst_spectra'
    """)
    print(f"Already have latest version ({last_ingested_version}) — nothing to do.")

# COMMAND ----------

# MAGIC %pip install astropy

# COMMAND ----------


import pandas as pd
from astropy.utils.data import download_file

new_catalogue_url = f"{URL_PREFIX}/{catalogue_name}_{latest_available}.csv.gz"
CACHE_DOWNLOADS = True

local_path = download_file(new_catalogue_url, cache=CACHE_DOWNLOADS)
jwst_pdf = pd.read_csv(local_path, compression="gzip")  # pandas auto-detects .gz compression from the extension

df_new = spark.createDataFrame(jwst_pdf)
df_new = df_new.withColumn("catalogue_version", F.lit(latest_available))

(
    df_new.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("alma_lakehouse.bronze.jwst_spectra_raw")
)

print(f"Ingested {catalogue_name}_{latest_available} into bronze.jwst_spectra_raw")


# COMMAND ----------

spark.sql("SHOW TABLES IN alma_lakehouse.bronze").display()

# COMMAND ----------

spark.sql("SHOW TABLES IN alma_lakehouse.control").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage note
# MAGIC ### ALMA
# MAGIC Source: `ecogal_prior_all_v1.csv` (local ALMA source catalogue export)
# MAGIC Target: `alma_lakehouse.bronze.sources_raw_prior`
# MAGIC Transformation: none (faithful raw copy)
# MAGIC ### JWST
# MAGIC Source: `dja_msaexp_emission_lines_v4.5` (latest DJA spectra catalogue from the AWS bucket)
# MAGIC Target: `alma_lakehouse.bronze.jwst_spectra_raw`
# MAGIC Transformation: none (faithful raw copy)