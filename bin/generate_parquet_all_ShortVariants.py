#!/usr/bin/env python3

# Florian Bénitière 16/03/2025
# Script to generate a .parquet file by merging all TSV gVCF files from a directory


import os
import shutil
import sys
import pandas as pd
import subprocess
import psutil  # System and process utilities
from math import floor  # For rounding down numbers
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_extract, input_file_name, lit, coalesce
import time
from tqdm import tqdm  # Progress bar library
import math

# Read arguments
tsv_dir_path = sys.argv[1]  # Directory containing TSV files
parquet_output = sys.argv[2]  # Output Parquet file path
cpus = int(sys.argv[3])  # Number of CPUs to use
mem_per_cpu = int(sys.argv[4])  # Memory per CPU in GB

######################   Initialize Spark session   #################################
total_memory = cpus * mem_per_cpu  # Total memory in GB based on CPU count

# Print system resource allocation
print("Total memory allocated (GB):", total_memory)
print("Number of CPUs allocated:", cpus)

# Set Java memory options for Spark to avoid memory issues with large datasets
os.environ["JAVA_TOOL_OPTIONS"] = f"-Xmx{total_memory}g"

# Initialize Spark session
spark = SparkSession.builder.appName("SPARK generate_schema_details") \
.config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
.config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
.config("spark.driver.cores", f"{cpus}") \
.config("spark.driver.memory", f"{total_memory}g") \
.getOrCreate()

spark.sparkContext.setLogLevel("WARN")
#######################################################

# Use `find` command to locate all compressed TSV files
find_command = f"find {tsv_dir_path} -type f -name '*.tsv.gz'"
result = subprocess.run(find_command, shell=True, capture_output=True, text=True)
subset_list_files = result.stdout.strip().split("\n") if result.stdout else []

# Start processing files
start_time = time.time()
first_file = True  # Flag to track first file processing

for file in tqdm(subset_list_files, desc="Processing files", unit="file", miniters = math.ceil(len(subset_list_files) / 100)):
    # Read TSV file into Spark DataFrame with inferred schema
    ShortVariants_type_data = spark.read.option("delimiter", "\t") \
        .option("nullValue", "-") \
        .csv(file, header=True, inferSchema=True) \
        .withColumn("SampleID", regexp_extract(input_file_name(), r"/([^/]+).tsv.gz$", 1))  # Extract sample name

    # Reorder columns to place 'SampleID' first
    new_column_order = ["SampleID"] + [c for c in ShortVariants_type_data.columns if c != "SampleID"]
    ShortVariants_type_data = ShortVariants_type_data.select(new_column_order)
    
    # Split AD column into REF_AD and ALT_AD
    ShortVariants_type_data = ShortVariants_type_data.withColumn("REF_AD", split(col("AD"), ",")[0].cast("int")) \
                                 .withColumn("ALT_AD", split(col("AD"), ",")[1].cast("int")) \
                                 .drop("AD")  # Drop original AD column if not needed

    # Write to Parquet file (overwrite for the first file, append for the rest)        
    if first_file:
        ShortVariants_type_data.write.mode("overwrite").parquet(parquet_output)
        first_file = False
    else:
        # Save each file in append mode
        ShortVariants_type_data.write.mode("append").parquet(parquet_output)


# End timing
end_time = time.time()
elapsed_time = end_time - start_time

# Final output messages
print(f"Data successfully written to {parquet_output}")
print(f"Total execution time: {elapsed_time:.2f} seconds")