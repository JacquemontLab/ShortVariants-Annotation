#!/usr/bin/env python3

# Florian Bénitière 16/03/2025
# This script generates a, unfiltered annotated .parquet file by integrating variant effect predictor (VEP) annotations,
# including all specified VEP plugins, and linking them to individual identifiers (SampleID).


import os
import sys
import pandas as pd
import subprocess
import psutil  # System and process utilities
from math import floor  # For rounding down numbers
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_extract, input_file_name, lit, coalesce, when, expr, concat_ws
import time
from tqdm import tqdm  # Progress bar library


# Parquet output path
all_ShortVariants_unannotated_path = sys.argv[1]  # Path to unannotated short variants (SNVs and Indels) Parquet file
vep_default_path = sys.argv[2]  # Path to default VEP annotations
list_plugins = sys.argv[3].split(",")  # List of plugin Parquet files
parquet_output = sys.argv[4]  # Output Parquet file path
cpus = int(sys.argv[5])  # Number of CPUs
mem_per_cpu = float(sys.argv[6])  # Memory per CPU (GB)

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

# Start execution timer
start_time = time.time()


# Load VEP default annotations
# Load the database
vep_annotation = spark.read.parquet(vep_default_path)

## Filtering on consequence of the MANE or CANONICAL, keeping only those that are not null MANE or not null CANONICAL
vep_annotation = vep_annotation.filter(
    (col("MANE").isNotNull()) | (col("CANONICAL").isNotNull())
)


# Load unannotated short variants (SNVs and Indels) database
print("START READING", all_ShortVariants_unannotated_path)
all_ShortVariants_unannotated = spark.read.parquet(all_ShortVariants_unannotated_path)

# Get the list of unique chromosomes from the VCF dataframe
chromosomes = all_ShortVariants_unannotated.select("CHROM").distinct().rdd.flatMap(lambda x: x).collect()

# Start processing files
start_time = time.time()
first_file = True  # Flag to track first file processing

for chrom in tqdm(chromosomes, desc="Processing chromosomes"):
    print(f"Processing chromosome {chrom}")
    
    all_ShortVariants_unannotated_chr = all_ShortVariants_unannotated.filter(col("CHROM") == chrom)
    vep_annotation_chr = vep_annotation.filter(col("Uploaded_variation") == chrom)
    
    # Columns to exclude from plugin files
    columns_to_exclude = ["Uploaded_variation","Location", "Allele", "Gene", "Feature"]

    # Load and merge plugin Parquet files
    for plugin_file in tqdm(list_plugins, desc="Processing Plugin"):
        plugins_parquet = spark.read.parquet(plugin_file)
        plugins_parquet = plugins_parquet.drop(*columns_to_exclude)  # Remove unwanted columns
        vep_annotation_chr = vep_annotation_chr.join(plugins_parquet, on="ID", how="left")
    
    # Create an ID column by concatenating CHROM and POS, this part is difficult and his here to produce a shared ID with VEP output, because VEP reformat the ID
    all_ShortVariants_unannotated_chr = all_ShortVariants_unannotated_chr.withColumn(
        "REF_mod",
        when((expr("LENGTH(REF) = 1") & (expr("LENGTH(ALT) > 1"))), lit(None))
        .when((expr("LENGTH(ALT) = 1") & (expr("LENGTH(REF) > 1"))), expr("SUBSTRING(REF, 2, LENGTH(REF))"))
        .otherwise(col("REF"))
    ).withColumn(
        "ALT_mod",
        when((expr("LENGTH(REF) = 1") & (expr("LENGTH(ALT) > 1"))), expr("SUBSTRING(ALT, 2, LENGTH(ALT))"))
        .when((expr("LENGTH(ALT) = 1") & (expr("LENGTH(REF) > 1"))), lit(None))
        .otherwise(col("ALT"))
    ).withColumn(
        "START",
        when((col("ALT_mod").isNull()), col("POS") + 1 )
        .otherwise(col("POS"))
    ).withColumn(
        "END",
        when((col("REF_mod").isNull()), col("POS") + 1 )
        .when((col("ALT_mod").isNull() & expr("LENGTH(REF_mod) = 1")), lit(None))
        .when((col("ALT_mod").isNull()), col("POS") + expr("LENGTH(REF_mod)"))
        .otherwise(lit(None))
    )

    # Create a unique variant ID column
    all_ShortVariants_unannotated_chr = all_ShortVariants_unannotated_chr.withColumn("ID", concat_ws(":", col("CHROM"),concat_ws("-", col("START"), col("END")), col("ALT_mod")))
    vep_annotation_chr = vep_annotation_chr.withColumn("ID", concat_ws(":", col("Location"), col("Allele")))


    # Identify shared and unique IDs between datasets
    ShortVariants_ids = all_ShortVariants_unannotated_chr.select("ID")
    vep_annot_ids = vep_annotation_chr.select("ID")

    shared_ids = ShortVariants_ids.intersect(vep_annot_ids)
    unique_ShortVariants = ShortVariants_ids.subtract(vep_annot_ids)  # IDs unique to pyspk
    unique_vep = vep_annot_ids.subtract(ShortVariants_ids)  # IDs unique to ShortVariants_type_data

    # Count and print ID distribution
    num_shared = shared_ids.count()
    num_unique_ShortVariants = unique_ShortVariants.count()
    num_unique_vep = unique_vep.count()

    print(f"Shared IDs: {num_shared}")
    print(f"Unique to short variants: {num_unique_ShortVariants}")
    print(f"Unique to vep_annotation_chr: {num_unique_vep}")


    # Merge annotations with short variants
    ShortVariants_annotated_chr = all_ShortVariants_unannotated_chr.join(vep_annotation_chr, on="ID", how="inner")

    # Drop useless columns
    ShortVariants_annotated_chr = ShortVariants_annotated_chr.drop("REF_mod", "ALT_mod", "START", "END", "Uploaded_variation", "Location", "Allele")

    # Write to Parquet file (overwrite for the first file, append for the rest)
    if first_file:
        ShortVariants_annotated_chr.write.partitionBy("CHROM").parquet(parquet_output, mode="overwrite")
        first_file = False
    else:
        # Save each file in append mode
        ShortVariants_annotated_chr.write.partitionBy("CHROM").mode("append").parquet(parquet_output)


    print(f"Number of total short variants: {ShortVariants_annotated_chr.count()}")
    print(f"Number of total short variants after VEP Annotation: {ShortVariants_annotated_chr.count()}")
    
    # Unpersist memory
    all_ShortVariants_unannotated_chr.unpersist()
    vep_annotation_chr.unpersist()
    ShortVariants_annotated_chr.unpersist()
    

# End execution timer
end_time = time.time()
elapsed_time = end_time - start_time

print(f"Data successfully written to {parquet_output}")
print(f"Total execution time: {elapsed_time:.2f} seconds")
