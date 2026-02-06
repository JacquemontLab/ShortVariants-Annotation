# Florian Bénitière - 21/03/2025
# This script processes unique UKBB gVCF files to filter SNPs, INDELs, and non-homozygous ref sites.

#!/bin/bash

# Exit immediately if a command exits with a non-zero status, if an undefined variable is used, or if a command in a pipeline fails
# set -euo pipefail


# Define input arguments
sample=$1  # Sample name
input_gvcf=$2  # Path to the gVCF input file
output=$3
fasta_ref=$4
cpu=${5:-$(nproc)}  # Use provided CPU count or default to the number of available processors

echo "Processing sample: ${sample}"
echo "Threads used: ${cpu}"

# Check if required input files exist before proceeding
for file in "$input_gvcf" "$fasta_ref"; do
    if [ ! -f "$file" ]; then
        echo "Error: File $file not found." >&2
        exit 1
    fi
done

# Check if required tools are installed (bcftools, tabix, bgzip)
for cmd in bcftools tabix bgzip; do
    if ! command -v $cmd &> /dev/null; then
        echo "Error: $cmd is not installed." >&2
        exit 1
    fi
done

# Create a random temporary directory that is automatically cleaned up on exit
tempdir_path=$(mktemp -d -t extraction_snps_indels_${sample}_XXXXXX)
trap "rm -rf ${tempdir_path}" EXIT  # Ensure cleanup on exit


# Create a directory for storing intermediate and final files
extension_path=${tempdir_path}${sample}

# Extract SNPs and Indels from the gVCF input file, normalize, filter non-homozygous ref variants and ./., and compress the output
bcftools view -v snps,indels --threads ${cpu} ${input_gvcf} | \
    bcftools norm -m- --threads ${cpu} | \
    bcftools view -v snps,indels -e 'GT="0/0" || GT="0|0" || GT="./." || GT=".|." || FMT/DP="." || FMT/AD="." || FMT/GQ="."' --threads ${cpu} | \
    bcftools norm -f ${fasta_ref} --threads ${cpu} | \
    bcftools view -Oz -o ${extension_path}_gvcf.vcf.gz

# Index the compressed VCF file
tabix -f ${extension_path}_gvcf.vcf.gz

# Extract variant information from the unique position VCF file and save as a tab-separated file
(echo -e "CHROM\tPOS\tREF\tALT\tGT\tDP\tAD\tGQ"; \
 bcftools query -f '%CHROM\t%POS\t%REF\t%ALT\t[%GT\t%DP\t%AD\t%GQ]\n' ${extension_path}_gvcf.vcf.gz) > \
 ${output%.gz}


# Compress the TSV file
bgzip -f ${output%.gz}


# Print the number of lines in the compressed TSV file
echo $(zcat ${output} | wc -l)


# Print the count of unique genotype occurrences
echo $(zcat ${output} | cut -f 5 | sort | uniq -c)