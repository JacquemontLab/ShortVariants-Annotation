#!/usr/bin/env python3

# Florian Bénitière 16/03/2025
# This script processes VEP outputs per chromosome, extracts necessary columns
# and merges them into a PySpark DataFrame before saving the output as a Parquet file.


import os
import sys
import pandas as pd
import subprocess
import psutil  # System and process utilities
from math import floor  # For rounding down numbers
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_extract, input_file_name, expr, concat_ws, greatest
import time
from tqdm import tqdm  # Progress bar library



# Read arguments
input_vcf_dir = sys.argv[1]  # Directory containing VCF files
parquet_output = sys.argv[2]  # Output Parquet file path
cpus = int(sys.argv[3])  # Number of CPU cores to use
mem_per_cpu = int(sys.argv[4])  # Memory allocated per CPU (GB)
plugin = sys.argv[5] # Column containing plugin annotations


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



# Use `find` command to locate all compressed TSV files in the directory
find_command = f"find -L {input_vcf_dir} -type f -name '*.tsv.gz'"

# Run the command and capture output
result = subprocess.run(find_command, shell=True, capture_output=True, text=True)

# Extract file paths from command output
subset_list_files = result.stdout.strip().split("\n") if result.stdout else []
print(f"Found {len(subset_list_files)} files:")

# Extract column headers from the first file using `zgrep`
file = subset_list_files[0]
result = subprocess.run(["zgrep", "^#", file], capture_output=True, text=True)

# Process the last header line found
if result.stdout:
    last_header_line = result.stdout.strip().split("\n")[-1].lstrip("#").split("\t")
    print(last_header_line)  # List of column names from the last header
else:
    print("No header line found!")


# Read and merge all files in a single loop
merged_df = None

start_time = time.time()
for file in tqdm(subset_list_files, desc="Processing files", unit="file"):
    
    # Read each TSV file into a PySpark DataFrame, treating "-" as NULL
    ShortVariants_type_data = (
        spark.read.option("delimiter", "\t")
        .option("nullValue", "-")
        .option("comment", "#")
        .csv(file, header=False, inferSchema=True)
        .toDF(*last_header_line)  # Assign column names
    )
    
    # Initialize merged DataFrame or merge with the existing one
    merged_df = ShortVariants_type_data if merged_df is None else merged_df.unionByName(ShortVariants_type_data)




# This is the complex part to read and reformat column that are specific per plugin, or do nothing if this is the default vep annotated file produced
if plugin == "spliceai":
    # Filter out rows with NULL plugin values
    merged_df = merged_df.filter(col("SpliceAI_pred").isNotNull()) 

    # Split the plugin column by "|" into an array
    merged_df = merged_df.withColumn("split_col", split(merged_df["SpliceAI_pred"], "\\|"))
    
    # Dynamically create new columns using a loop
    for i, column_name in enumerate("SYMBOL|DS_AG|DS_AL|DS_DG|DS_DL|DP_AG|DP_AL|DP_DG|DP_DL".split('|')):
        merged_df = merged_df.withColumn(column_name, col("split_col")[i])

    # Drop intermediate columns
    merged_df = merged_df.drop("SpliceAI_pred", "split_col")
    
elif plugin == "alphamissense":
    # Drop short variant if all columns are NULL
    condition = " OR ".join([f"{col} IS NOT NULL" for col in "am_class,am_pathogenicity".split(",")])
    merged_df = merged_df.filter(expr(condition))
    
elif plugin == "loftee":
    # Drop short variant if all columns are NULL
    condition = " OR ".join([f"{col} IS NOT NULL" for col in "LoF,LoF_filter,LoF_flags,LoF_info".split(",")])
    merged_df = merged_df.filter(expr(condition))

elif plugin == "default":
    # List of all gnomAD AF columns
    gnomad_cols = [
        "gnomADe_AF", "gnomADe_AFR_AF", "gnomADe_AMR_AF", "gnomADe_ASJ_AF", "gnomADe_EAS_AF",
        "gnomADe_FIN_AF", "gnomADe_MID_AF", "gnomADe_NFE_AF", "gnomADe_REMAINING_AF", "gnomADe_SAS_AF",
        "gnomADg_AF", "gnomADg_AFR_AF", "gnomADg_AMI_AF", "gnomADg_AMR_AF", "gnomADg_ASJ_AF",
        "gnomADg_EAS_AF", "gnomADg_FIN_AF", "gnomADg_MID_AF", "gnomADg_NFE_AF", "gnomADg_REMAINING_AF",
        "gnomADg_SAS_AF"
    ]

    # Create a new column with the max AF across all the gnomAD AF columns
    merged_df = merged_df.withColumn("gnomAD_max_AF", greatest(*[col(c) for c in gnomad_cols]))
    
    # Drop intermediate columns
    merged_df = merged_df.drop(*gnomad_cols)
    
    # Convert null AF value to 0
    merged_df = merged_df.fillna({'gnomAD_max_AF': 0.0})
    merged_df = merged_df.fillna({'MAX_AF': 0.0})


# Create a unique identifier column using Location, Allele, Gene, and Feature
merged_df = merged_df.withColumn("ID", concat_ws(":", col("Location"), col("Allele"), col("Gene"), col("Feature")))


# Display some output statistics
merged_df.printSchema()
merged_df.count()
merged_df.show()

# Save the final DataFrame as a Parquet file
merged_df.write.partitionBy("Uploaded_variation").parquet(parquet_output, mode="overwrite")


# Calculate execution time
end_time = time.time()
elapsed_time = end_time - start_time

print(f"Data successfully written to {parquet_output}")
print(f"Total execution time: {elapsed_time:.2f} seconds")