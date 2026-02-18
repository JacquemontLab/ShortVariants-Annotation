#!/bin/bash
# ==============================================================================
# Author:       Florian Bénitière
# Date:         21/03/2025
# Description:  Normalizes and filters unique gVCF files. 
#               - Targets: Autosomes (1-22) and Sex Chromosomes (X, Y)
#               - Filters: SNPs/INDELs only; excludes Hom-Ref (0/0) and missing 
#                 genotypes/metrics (./., DP, AD, GQ).
# ==============================================================================

# Exit immediately if a command exits with a non-zero status, if an undefined variable is used, or if a command in a pipeline fails
set -euo pipefail


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

# Generate the index file (creates .tbi)
bcftools index -t ${input_gvcf}

# Extract and normalize SNPs/Indels, filter out homozygous ref and missing genotypes or missing metrics (DP=depth, AD=allele depth, GQ=genotype quality), and compress output
bcftools view -v snps,indels -r chr1,chr2,chr3,chr4,chr5,chr6,chr7,chr8,chr9,chr10,chr11,chr12,chr13,chr14,chr15,chr16,chr17,chr18,chr19,chr20,chr21,chr22,chrX,chrY --threads ${cpu} ${input_gvcf} | \
    bcftools norm -m- --threads ${cpu} --force | \
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
