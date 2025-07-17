# Florian Bénitière 16/03/2025
# Script to generate a .parquet file by merging all batch-level Parquet files


import os
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
batches_files = sys.argv[1].split(",")  # Directory containing TSV files
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

# Start processing files
start_time = time.time()
first_file = True  # Flag to track first file processing

for file in tqdm(batches_files, desc="Processing files", unit="file", miniters = math.ceil(len(batches_files) / 100)):
    # Load the Parquet file using the custom PySpark loader
    pyspk = spark.read.parquet(file)
    
    # Write to Parquet file (overwrite for the first file, append for the rest)
    if first_file:
        pyspk.write.parquet(parquet_output, mode="overwrite")
        first_file = False
    else:
        # Save each file in append mode
        pyspk.write.mode("append").parquet(parquet_output)

# End timing
end_time = time.time()
elapsed_time = end_time - start_time

# Final output messages
print(f"Data successfully written to {parquet_output}")
print(f"Total execution time: {elapsed_time:.2f} seconds")