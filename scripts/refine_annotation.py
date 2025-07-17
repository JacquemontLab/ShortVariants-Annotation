# Florian Bénitière 16/03/2025
# This script processes annotated SNV (Single Nucleotide Variant) data stored in Parquet format
# using PySpark. It performs multiple steps including:
# 
# 1. **Chromosome-wise Processing**: Iterates over each chromosome to:
#    - Convert SpliceAI scores to double precision.
#    - Count unique individuals (SampleID) and SNVs.
#    - Apply quality control filters based on sequencing depth (DP), genotype quality (GQ), and allele balance (AC ratio).
#    - Filter rare variants (Max AF ≤ 0.1%).
#    - Annotate SNVs based on their predicted functional consequences.
#    - Apply additional filtering for pathogenicity scores and SpliceAI predictions.
# 2. **Data Export**: Saves the filtered SNV data to an output Parquet file, partitioned by chromosome.
# 3. **Logging**: Generates a summary report detailing the number of SNVs retained at each stage.


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
import sys
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.functions import when, expr, col, concat_ws, lit
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import min, max
from pyspark.sql.functions import greatest
from tqdm import tqdm  # Progress bar library


# Parquet output path
input_parquet_annotation = sys.argv[1]  # Path to unannotated SNV Parquet file
report_output = sys.argv[2]  # Path to default VEP annotations
parquet_output = sys.argv[3]  # Output Parquet file path
cpus = int(sys.argv[4])  # Number of CPUs
mem_per_cpu = int(sys.argv[5])  # Memory per CPU (GB)

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


with open(report_output, 'w') as file:  # 'w' mode will overwrite the file
    file.write('\n')

# Function to write logs to a file
def write_to_file(file_path, text):
    with open(file_path, 'a') as file:
        file.write(text + '\n')


write_to_file(report_output, "\n---- SNV Summary Report ----\n")

# Load Parquet file
snv_annotated = spark.read.parquet(input_parquet_annotation)
snv_annotated.printSchema()

# Get the list of unique chromosomes
chromosomes = snv_annotated.select("CHROM").distinct().rdd.flatMap(lambda x: x).collect()

# Start processing files
start_time = time.time()
first_file = True  # Flag to track first file processing

for chrom in tqdm(chromosomes, desc="Processing chromosomes"):
    print(f"Processing chromosome {chrom}")
    
    write_to_file(report_output, f"\nProcessing Chromosome {chrom}...\n")
    # Filter by chromosome to reduce memory usage
    snv_chrom = snv_annotated.filter(snv_annotated["CHROM"] == chrom)
    
    # Count unique SNV SampleID
    unique_iid_count = snv_chrom.select(countDistinct("SampleID")).collect()[0][0]

    # Count unique SNV ID
    unique_snv_count = snv_chrom.select(countDistinct("ID")).collect()[0][0]

    # Count unique (SampleID, SNV_ID) pairs
    unique_iid_snv_count = snv_chrom.select(countDistinct("SampleID", "ID")).collect()[0][0]

    # Summary for unique counts
    write_to_file(report_output, f"SampleID Count: {unique_iid_count}")
    write_to_file(report_output, f"Unique SNV Count: {unique_snv_count}")
    write_to_file(report_output, f"SNV Count: {unique_iid_snv_count}")
    
    # Assuring the double format for these specific variables of SpliceAI
    snv_chrom = snv_chrom.withColumn("DS_AG", col("DS_AG").cast("double")) \
       .withColumn("DS_AL", col("DS_AL").cast("double")) \
       .withColumn("DS_DG", col("DS_DG").cast("double")) \
       .withColumn("DS_DL", col("DS_DL").cast("double"))


    ### Call quality control filtering

    # Create AC ratio (ALT_AD / DP)
    snv_chrom = snv_chrom.withColumn("AC_ratio", col("ALT_AD") / (col("DP")))

    # Filter SNVs with DP >= 20 and GQ >= 30 and 0.2 <= AC_ratio <= 0.8
    snv_chrom = snv_chrom.filter((snv_chrom.DP >= 20) & (snv_chrom.GQ >= 30) & (snv_chrom.AC_ratio >= 0.2) & (snv_chrom.AC_ratio <= 0.8) )

    # Count unique (SampleID, SNV_ID) pairs
    unique_iid_snv_count_qc = snv_chrom.select(countDistinct("SampleID", "ID")).collect()[0][0]

    # Summary for QC counts
    write_to_file(report_output, f"\nSNV Count after QC (DP >= 20 and GQ >= 30 and 0.2 <= AC_ratio <= 0.8): {unique_iid_snv_count_qc}\n")



    # Compute dataset_AF Group by variant and count distinct SampleIDs
    variant_iid_counts = snv_chrom.groupBy("POS", "REF", "ALT") \
        .agg((countDistinct("SampleID") / lit(unique_iid_count)).alias("dataset_AF"))
    
    # Join back to original data
    snv_chrom = snv_chrom.join(variant_iid_counts, on=["POS", "REF", "ALT"], how="left")


    ### Rare variant filtering

    # Filter SNVs with gnomAD_max_AF <= 0.1%
    snv_chrom = snv_chrom.filter((snv_chrom.gnomAD_max_AF <= 0.001) & (snv_chrom.dataset_AF <= 0.001) )

    # Count unique (SampleID, SNV_ID) pairs
    unique_iid_snv_count_qc_rare = snv_chrom.select(countDistinct("SampleID", "ID")).collect()[0][0]

    # Summary for rare variant counts
    write_to_file(report_output, f"\nSNV Count after Rare Variant QC (gnomAD_max_AF & dataset_AF <= 0.001): {unique_iid_snv_count_qc_rare}\n")

    snv_chrom.printSchema()

    # Extract the first string before a comma in the 'Consequence' column
    snv_chrom = snv_chrom.withColumn("First_Consequence", split(snv_chrom["Consequence"], ",")[0])

    # Annotate Variant Type
    snv_chrom = snv_chrom.withColumn(
        "Variant_Type",
        when(snv_chrom["Consequence"] == "synonymous_variant", "Synonymous")
        .when(snv_chrom["First_Consequence"] == "stop_gained", "Stop_Gained")
        .when(snv_chrom["First_Consequence"] == "frameshift_variant", "Frameshift")
        .when(snv_chrom["First_Consequence"] == "splice_acceptor_variant", "Splice_variants")
        .when(snv_chrom["First_Consequence"] == "splice_donor_variant", "Splice_variants")
        .when(snv_chrom["First_Consequence"] == "missense_variant", "Missense")
        .otherwise("Other")
    )

    # Count occurrences of each Variant_Type
    variant_type_counts = snv_chrom.groupBy("Variant_Type").count()

    # Filter the data for "Other" variant type
    other_variant_df = snv_chrom.filter(snv_chrom["Variant_Type"] == "Other")

    # Count occurrences of each Consequence within the "Other" Variant_Type
    consequence_counts = other_variant_df.groupBy("Consequence").count()

    # Show the entire count of each Consequence for "Other" Variant_Type
    consequence_counts.orderBy(col("count").desc()).show(truncate=False)

    # Write the Variant Type Counts before filtering to file
    write_to_file(report_output, "\nVariant Type Counts Before Filtering:")
    for row in variant_type_counts.collect():
        write_to_file(report_output, f"{row['Variant_Type']}: {row['count']}")

    # Create a new column 'Max_DS' with the maximum value of DS_AG, DS_AL, DS_DG, and DS_DL
    snv_chrom = snv_chrom.withColumn(
        "Max_DS_SpliceAI",
        greatest(
            col("DS_AG"),
            col("DS_AL"),
            col("DS_DG"),
            col("DS_DL")
        )
    )

    # Filter conditions:
    snv_chrom = snv_chrom.filter(
        ((snv_chrom["Variant_Type"].isin("Stop_Gained", "Frameshift")) & (snv_chrom["LoF"].isin("HC"))) |
        ((snv_chrom["Variant_Type"].isin("Splice_variants")) & (snv_chrom["Max_DS_SpliceAI"] >= 0.8)) |
        ((snv_chrom["Variant_Type"].isin("Missense")) & (snv_chrom["am_pathogenicity"] >= 0.564)) |
        (snv_chrom["Variant_Type"].isin("Synonymous"))
    )

    # Show the filtered result
    snv_chrom.show()

    # Count occurrences of each Variant_Type
    variant_type_counts = snv_chrom.groupBy("Variant_Type").count()

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

    snv_chrom = snv_chrom.select(columns_to_keep)

    # Write to Parquet file (overwrite for the first file, append for the rest)
    if first_file:
        snv_chrom.write.partitionBy("CHROM").parquet(parquet_output, mode="overwrite")
        first_file = False
    else:
        # Save each file in append mode
        snv_chrom.write.partitionBy("CHROM").mode("append").parquet( parquet_output )
    snv_chrom.unpersist()

snv_chrom.printSchema()
# Final summary report
write_to_file(report_output, "\n--- End Summary ---")