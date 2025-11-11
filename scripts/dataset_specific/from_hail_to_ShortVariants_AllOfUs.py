import hail as hl
import os
import time

hl.init()

from datetime import datetime
start = datetime.now()

bucket = os.getenv("WORKSPACE_BUCKET")
bucket


# hardcode the VDS path in case you are not in a v8 workspace bucket
vds_srwgs_path = "gs://fc-aou-datasets-controlled/v8/wgs/short_read/snpindel/exome/splitMT/hail.mt"
mt = hl.read_matrix_table(vds_srwgs_path)

# Inspect first 5 rows
mt.show(5)

# Show first 5 entries (variant × sample combinations)
mt.entries().show(5)

# Count unique SampleIDs
num_samples = mt.cols().count()
print(f"Number of unique SampleIDs: {num_samples}")

# Select only entry fields we care about
mt = mt.select_entries("GT", "AD", "GQ")

# Filter to heterozygous or homozygous alternate genotypes
# Non Homozygous reference sites — Removing 0/0 or ./. to keep only variant sites.
mt = mt.filter_entries(mt.GT.is_het() | mt.GT.is_hom_var())

# Flatten to entries (one row per sample × variant)
tbl = mt.entries()

# Annotate columns
tbl = tbl.annotate(
    SampleID = tbl.s,
    CHROM = tbl.locus.contig,
    POS = tbl.locus.position,
    REF = tbl.alleles[0],
    ALT = tbl.alleles[1],
    DP = hl.sum(tbl.AD),
    REF_AD = tbl.AD[0],
    ALT_AD = tbl.AD[1],
    GT = tbl.GT
)

# Select and reorder columns exactly as desired
tbl = tbl.select("SampleID", "CHROM", "POS", "REF", "ALT", "GT", "DP", "GQ", "REF_AD", "ALT_AD")


tbl_small = tbl.head(10)
tbl_small.head(2).show()

start_time = time.time()
# Save as Parquet

output_file="gs://fc-secure-af0d21bb-d24b-4548-8b67-48ee7f4cbc56/UPSTREAM_ANALYSIS/AllofUs_tierv8_ShortVariants_annot_intermediate/processed/Unannotated_ShortVariants_intermediate"

tbl.to_spark().write.parquet(output_file + ".parquet")

end_time = time.time()
elapsed = end_time - start_time

print(f"Writing Parquet took {elapsed:.2f} seconds")



import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

spark = SparkSession.builder \
    .appName("ConvertGTParquet") \
    .config("spark.driver.memory", "600g") \
    .config("spark.executor.memory", "600g") \
    .getOrCreate()

# Input and output paths
input_path = "Unannotated_ShortVariants_intermediate.parquet"
output_path = "Unannotated_ShortVariants.parquet"

# Read the Parquet dataset (can be a directory)
df = spark.read.parquet(input_path)

# Convert and drop
df_converted = (
    df.withColumn("GT", concat_ws("/", col("`GT.alleles`")))  # ✅ backticks here
      .drop("GT.alleles", "GT.phased")                        # ✅ no backticks here
)


# Reorder columns
df_reordered = df_converted.select(
    "SampleID",  # or "IID" if your ID column is named that
    "CHROM",
    "POS",
    "REF",
    "ALT",
    "GT",
    "DP",
    "GQ",
    "REF_AD",
    "ALT_AD"
)

# Display result
df_reordered.show()
# Check the number of partitions
num_partitions = df_reordered.rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")

# Write back to Parquet
df_reordered.write.mode("overwrite").parquet(output_path)

