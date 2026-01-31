#!/usr/bin/env python3

# Florian Bénitière 16/03/2025
# This script processes annotated short variants (SNVs and Indels) data stored in Parquet format
# using PySpark. It performs multiple steps including:
# 
# 1. **Chromosome-wise Processing**: Iterates over each chromosome to:
#    - Convert SpliceAI scores to double precision.
#    - Count unique individuals (SampleID) and short variants.
#    - Apply quality control filters based on sequencing depth (DP), genotype quality (GQ), and allele balance (AC ratio).
#    - Filter rare variants (Max AF ≤ 0.1%).
#    - Annotate short variants based on their predicted functional consequences.
#    - Apply additional filtering for pathogenicity scores and SpliceAI predictions.
# 2. **Data Export**: Saves the filtered short variants data to an output Parquet file, partitioned by chromosome.
# 3. **Logging**: Generates a summary report detailing the number of short variants retained at each stage.


import os
import sys
import pandas as pd
import subprocess
import psutil  # System and process utilities
from math import floor  # For rounding down numbers
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit 
from pyspark.sql.functions import coalesce
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_extract, input_file_name
import time
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.functions import when, expr, col, concat_ws, lit
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import min, max
from pyspark.sql.functions import greatest
from tqdm import tqdm  # Progress bar library


# Parquet output path
input_parquet_annotation = sys.argv[1]  # Path to unfiltered short variants Parquet file
report_output = sys.argv[2]  # Path to default VEP annotations
parquet_output = sys.argv[3]  # Output Parquet file path
cpus = int(sys.argv[4])  # Number of CPUs
mem_per_cpu = float(sys.argv[5])  # Memory per CPU (GB)

######################   Initialize Spark session   #################################
total_memory = cpus * mem_per_cpu  # Total memory in GB based on CPU count
total_memory = int(total_memory)   # Convert to integer to avoid float formatting issues

# Print system resource allocation
print("Total memory allocated (GB):", total_memory)
print("Number of CPUs allocated:", cpus)

# Set Java memory options for Spark to avoid memory issues with large datasets
os.environ["JAVA_TOOL_OPTIONS"] = f"-Xmx{total_memory}g"

spark_local_dirs = os.getenv("SPARK_LOCAL_DIRS")
if not spark_local_dirs:  # catches both None and ""
    sys.exit("Error: SPARK_LOCAL_DIRS environment variable is not set.")

# Initialize Spark session
spark = SparkSession.builder.appName("SPARK generate_schema_details") \
    .config("spark.local.dir", spark_local_dirs) \
    .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={spark_local_dirs}") \
    .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={spark_local_dirs}") \
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
    .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
    .config("spark.driver.cores", f"{cpus}") \
    .config("spark.driver.memory", f"{total_memory}g") \
    .getOrCreate()

# Optional: check settings
print("spark.local.dir:", spark.conf.get("spark.local.dir"))
print("JVM java.io.tmpdir:", spark.sparkContext._jvm.System.getProperty("java.io.tmpdir"))

spark.sparkContext.setLogLevel("WARN")
#######################################################


with open(report_output, 'w') as file:  # 'w' mode will overwrite the file
    file.write('\n')

# Function to write logs to a file
def write_to_file(file_path, text):
    with open(file_path, 'a') as file:
        file.write(text + '\n')


write_to_file(report_output, "\n---- Short variants Summary Report ----\n")

# Load Parquet file
ShortVariants_annotated = spark.read.parquet(input_parquet_annotation)
ShortVariants_annotated.printSchema()

# Get the list of unique chromosomes
chromosomes = ShortVariants_annotated.select("CHROM").distinct().rdd.flatMap(lambda x: x).collect()

# Start processing files
start_time = time.time()
first_file = True  # Flag to track first file processing

for chrom in tqdm(chromosomes, desc="Processing chromosomes"):
    print(f"Processing chromosome {chrom}")
    
    write_to_file(report_output, f"\nProcessing Chromosome {chrom}...\n")
    # Filter by chromosome to reduce memory usage
    ShortVariants_chrom = ShortVariants_annotated.filter(ShortVariants_annotated["CHROM"] == chrom)
    
    # Count unique short variants SampleID
    unique_iid_count = ShortVariants_chrom.select(countDistinct("SampleID")).collect()[0][0]

    # Count unique short variants ID
    unique_ShortVariants_count = ShortVariants_chrom.select(countDistinct("ID")).collect()[0][0]

    # Count unique (SampleID, ShortVariants_ID) pairs
    unique_iid_ShortVariants_count = ShortVariants_chrom.select(countDistinct("SampleID", "ID")).collect()[0][0]

    # Summary for unique counts
    write_to_file(report_output, f"SampleID Count: {unique_iid_count}")
    write_to_file(report_output, f"Unique ShortVariants Count: {unique_ShortVariants_count}")
    write_to_file(report_output, f"ShortVariants Count: {unique_iid_ShortVariants_count}")
    
    # Assuring the double format for these specific variables of SpliceAI
    ShortVariants_chrom = ShortVariants_chrom.withColumn("DS_AG", col("DS_AG").cast("double")) \
       .withColumn("DS_AL", col("DS_AL").cast("double")) \
       .withColumn("DS_DG", col("DS_DG").cast("double")) \
       .withColumn("DS_DL", col("DS_DL").cast("double"))


    ### Call quality control filtering

    # Create AC ratio (ALT_AD / DP)
    ShortVariants_chrom = ShortVariants_chrom.withColumn("AC_ratio", col("ALT_AD") / (col("DP")))

    # Filter short variants with DP >= 20 and GQ >= 30 and 0.2 <= AC_ratio <= 0.8
    ShortVariants_chrom = ShortVariants_chrom.filter((ShortVariants_chrom.DP >= 20) & (ShortVariants_chrom.GQ >= 30) & (ShortVariants_chrom.AC_ratio >= 0.2) & (ShortVariants_chrom.AC_ratio <= 0.8) )

    # Count unique (SampleID, ShortVariants_ID) pairs
    unique_iid_ShortVariants_count_qc = ShortVariants_chrom.select(countDistinct("SampleID", "ID")).collect()[0][0]

    # Summary for QC counts
    write_to_file(report_output, f"\nShort variants Count after QC (DP >= 20 and GQ >= 30 and 0.2 <= AC_ratio <= 0.8): {unique_iid_ShortVariants_count_qc}\n")


    # Compute dataset_AF as allele frequency: group by variant and divide by 2 × total number of individuals
    variant_iid_counts = ShortVariants_chrom.groupBy("POS", "REF", "ALT") \
    .agg(
        (countDistinct("SampleID") / (lit(2) * lit(unique_iid_count))).alias("dataset_AF")
    )
    
    # Join back to original data
    ShortVariants_chrom = ShortVariants_chrom.join(variant_iid_counts, on=["POS", "REF", "ALT"], how="left")


    ### Rare variant filtering

    # Apply rare-variant filters based on cohort size
    # Threshold used: minor allele frequency ≤ 0.1% (0.001)

    if unique_iid_count >= 5000:
        # For large cohorts (≥ 5000 individuals),
        # enforce rarity both in external reference (gnomAD)
        # and within the studied dataset to avoid cohort-specific common variants
        ShortVariants_chrom = ShortVariants_chrom.filter(
            (ShortVariants_chrom.gnomAD_max_AF <= 0.001) &
            (ShortVariants_chrom.dataset_AF <= 0.001)
        )
    else:
        # For smaller cohorts,
        # rely only on gnomAD frequency since internal AF estimates
        # may be unstable due to limited sample size
        ShortVariants_chrom = ShortVariants_chrom.filter(
            ShortVariants_chrom.gnomAD_max_AF <= 0.001
        )

    # Count unique (SampleID, ShortVariants_ID) pairs
    unique_iid_ShortVariants_count_qc_rare = ShortVariants_chrom.select(countDistinct("SampleID", "ID")).collect()[0][0]

    # Summary for rare variant counts
    write_to_file(report_output, f"\nShort variants Count after Rare Variant QC (gnomAD_max_AF & dataset_AF <= 0.001): {unique_iid_ShortVariants_count_qc_rare}\n")

    ShortVariants_chrom.printSchema()

    # Extract the first string before a comma in the 'Consequence' column
    ShortVariants_chrom = ShortVariants_chrom.withColumn("First_Consequence", split(ShortVariants_chrom["Consequence"], ",")[0])

    # Annotate Variant Type
    ShortVariants_chrom = ShortVariants_chrom.withColumn(
        "Variant_Type",
        when(ShortVariants_chrom["Consequence"] == "synonymous_variant", "Synonymous")
        .when(ShortVariants_chrom["First_Consequence"] == "stop_gained", "Stop_Gained")
        .when(ShortVariants_chrom["First_Consequence"] == "frameshift_variant", "Frameshift")
        .when(ShortVariants_chrom["First_Consequence"] == "splice_acceptor_variant", "Splice_variants")
        .when(ShortVariants_chrom["First_Consequence"] == "splice_donor_variant", "Splice_variants")
        .when(ShortVariants_chrom["First_Consequence"] == "missense_variant", "Missense")
        .otherwise("Other")
    )

    # Count occurrences of each Variant_Type
    variant_type_counts = ShortVariants_chrom.groupBy("Variant_Type").count()

    # Filter the data for "Other" variant type
    other_variant_df = ShortVariants_chrom.filter(ShortVariants_chrom["Variant_Type"] == "Other")

    # Count occurrences of each Consequence within the "Other" Variant_Type
    consequence_counts = other_variant_df.groupBy("Consequence").count()

    # Show the entire count of each Consequence for "Other" Variant_Type
    consequence_counts.orderBy(col("count").desc()).show(truncate=False)

    # Write the Variant Type Counts before filtering to file
    write_to_file(report_output, "\nVariant Type Counts Before Filtering:")
    for row in variant_type_counts.collect():
        write_to_file(report_output, f"{row['Variant_Type']}: {row['count']}")

    # Create a new column 'Max_DS' with the maximum value of DS_AG, DS_AL, DS_DG, and DS_DL
    ShortVariants_chrom = ShortVariants_chrom.withColumn(
        "Max_DS_SpliceAI",
        greatest(
            col("DS_AG"),
            col("DS_AL"),
            col("DS_DG"),
            col("DS_DL")
        )
    )

    # Filter conditions:
    ShortVariants_chrom = ShortVariants_chrom.filter(
        ((ShortVariants_chrom["Variant_Type"].isin("Stop_Gained", "Frameshift")) & (ShortVariants_chrom["LoF"].isin("HC"))) |
        ((ShortVariants_chrom["Variant_Type"].isin("Splice_variants")) & (ShortVariants_chrom["Max_DS_SpliceAI"] >= 0.8)) |
        ((ShortVariants_chrom["Variant_Type"].isin("Missense")) & (ShortVariants_chrom["am_pathogenicity"] >= 0.564)) |
        (ShortVariants_chrom["Variant_Type"].isin("Synonymous"))
    )

    # Show the filtered result
    ShortVariants_chrom.show()

    # Count occurrences of each Variant_Type
    variant_type_counts = ShortVariants_chrom.groupBy("Variant_Type").count()

    # Write the Variant Type Counts before filtering to file
    write_to_file(report_output, "\nVariant Type Counts After Filtering:")
    for row in variant_type_counts.collect():
        write_to_file(report_output, f"{row['Variant_Type']}: {row['count']}")


    # Assuming df is your existing DataFrame
    columns_to_keep = [
        "SampleID", "CHROM", "POS", "REF", "ALT", "GT", "DP", "GQ",
        "REF_AD", "ALT_AD", "AC_ratio", "Gene", "Feature","CANONICAL","MANE",
        "dataset_AF","gnomAD_max_AF", "MAX_AF", "MAX_AF_POPS", 
        "Variant_Type", "am_pathogenicity", "Max_DS_SpliceAI"
    ]

    ShortVariants_chrom = ShortVariants_chrom.select(columns_to_keep)

    # Write to Parquet file (overwrite for the first file, append for the rest)
    if first_file:
        ShortVariants_chrom.write.partitionBy("CHROM").parquet(parquet_output, mode="overwrite")
        first_file = False
    else:
        # Save each file in append mode
        ShortVariants_chrom.write.partitionBy("CHROM").mode("append").parquet( parquet_output )
    ShortVariants_chrom.unpersist()

ShortVariants_chrom.printSchema()
# Final summary report
write_to_file(report_output, "\n--- End Summary ---")
