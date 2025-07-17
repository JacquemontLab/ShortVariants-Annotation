# Florian Bénitière 16/03/2025
# Script to find unique SNVs from a large .parquet file and produce VCF files per chromosome for use by VEP

import os
import sys
import pandas as pd
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_extract, input_file_name, coalesce, lit
from tqdm import tqdm  # Progress bar library


# Read arguments
parquet_input = sys.argv[1]  # Directory containing VCF files
output_dir = sys.argv[2]  # Output Parquet file path
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

# Load the database
pyspk = spark.read.parquet(parquet_input)

# Select unique variants based on chromosome, position, reference, and alternate alleles
unique_variants = pyspk.select("CHROM", "POS", "REF", "ALT").distinct()

# Count the number of unique variants per chromosome and sort by chromosome number
print("Number of unique:", unique_variants.count())

# Count number of rows per chromosome
chromosome_counts = (
    unique_variants.groupBy("CHROM")
    .count()
    .orderBy("CHROM")
)

# Display the chromosome-wise variant counts
chromosome_counts.show(n=chromosome_counts.count(), truncate=False)


# Prepare the VCF dataframe by selecting necessary fields and adding placeholders for VCF format
vcf_df = (
    pyspk.select(
        F.col("CHROM").alias("CHROM"),
        F.col("POS").alias("POS"),
        F.concat_ws(";", F.col("CHROM"), F.col("POS")).alias("ID"),  # Placeholder
        F.col("REF").alias("REF"),
        F.col("ALT").alias("ALT"),
        F.lit(".").alias("QUAL"),  # Placeholder
        F.lit("PASS").alias("FILTER"),  # Placeholder
        F.lit(".").alias("INFO")  # Placeholder
    )
    .distinct()
)

# Define VCF header
vcf_header = """##fileformat=VCFv4.2
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO"""

# Get the list of unique chromosomes from the VCF dataframe
chromosomes = vcf_df.select("CHROM").distinct().rdd.flatMap(lambda x: x).collect()

# Iterate over each chromosome and save as a separate VCF file
for chrom in tqdm(chromosomes, desc="Processing Chromosomes"):
    chrom_df = vcf_df.filter(F.col("CHROM") == chrom) # Filter variants belonging to the chromosome
    
    # Save as a tab-separated file (TSV)
    chrom_output_path = f"{output_dir}{chrom}"
    chrom_df.coalesce(1).write.option("sep", "\t").option("header", "false").mode("overwrite").csv(chrom_output_path)
    
    # Find the actual TSV file generated
    tsv_file = subprocess.check_output(f'ls {chrom_output_path}/part-*.csv', shell=True).decode().strip()
    
    # Define final VCF file path
    final_vcf_path = f"{output_dir}{chrom}.vcf"
    
    # Prepend the header and save as VCF
    subprocess.run(f"echo '{vcf_header}' | cat - {tsv_file} > {final_vcf_path}", shell=True)
    
    # Remove the temporary directory
    if os.path.exists(chrom_output_path) and os.path.isdir(chrom_output_path):
        subprocess.run(f"rm -rf {chrom_output_path}", shell=True)
    
    # Print confirmation message for the chromosome VCF file
    print(f"VCF file for chromosome {chrom} saved at: {final_vcf_path}")
    

