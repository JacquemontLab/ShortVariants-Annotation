# Florian Bénitière - 21/03/2025
# This script processes unique SPARK gVCF files to filter SNPs, INDELs, and non-homozygous ref sites.
# It then retains only short variants (SNVs and Indels) that intersect between GATK and DeepVariant, preserving DeepVariant metadata in the output.

#!/bin/bash

# Exit immediately if a command exits with a non-zero status, if an undefined variable is used, or if a command in a pipeline fails
set -euo pipefail


# Define input arguments
sample=$1  # Sample name
input_gatk=$2  # Path to the GATK input file
input_deepvariant=$3  # Path to the DeepVariant input file
output=$4
fasta_ref=$5
cpu=${6:-$(nproc)}  # Use provided CPU count or default to the number of available processors

echo "Processing sample: ${sample}"
echo "Threads used: ${cpu}"

# Check if required input files exist before proceeding
for file in "$input_gatk" "$input_deepvariant" "$fasta_ref"; do
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

# Extract SNPs and Indels from the GATK input file, normalize, filter non-homozygous ref variants and ./., and compress the output
bcftools view -v snps,indels --threads ${cpu} ${input_gatk} | \
    bcftools norm -m- --threads ${cpu} | \
    bcftools view -v snps,indels -e 'GT="0/0" | GT="./."' --threads ${cpu} | \
    bcftools norm -f ${fasta_ref} --threads ${cpu} | \
    bcftools view -Oz -o ${extension_path}_gatk.vcf.gz


# Index the compressed VCF file
tabix -f ${extension_path}_gatk.vcf.gz


# Extract SNPs and Indels from the DeepVariant input file, normalize, filter non-homozygous ref variants and ./., and compress the output
bcftools view -v snps,indels --threads ${cpu} ${input_deepvariant} | \
    bcftools norm -m- --threads ${cpu} | \
    bcftools view -v snps,indels -e 'GT="0/0" | GT="./."' --threads ${cpu} | \
    bcftools norm -f ${fasta_ref} --threads ${cpu} | \
    bcftools view -Oz -o ${extension_path}_deepvariant.vcf.gz

# Index the compressed VCF file
tabix -f ${extension_path}_deepvariant.vcf.gz


# Identify positions that are present in both DeepVariant and GATK results
bcftools isec ${extension_path}_deepvariant.vcf.gz \
              ${extension_path}_gatk.vcf.gz \
              --threads ${cpu} -n=2 -w 1 -Oz -o ${extension_path}_shared_pos.vcf.gz


# Extract variant information from the unique position VCF file and save as a tab-separated file
(echo -e "CHROM\tPOS\tREF\tALT\tGT\tDP\tAD\tGQ"; \
 bcftools query -f '%CHROM\t%POS\t%REF\t%ALT\t[%GT\t%DP\t%AD\t%GQ]\n' ${extension_path}_shared_pos.vcf.gz) > \
 ${output%.gz}

# Compress the TSV file
bgzip -f ${output%.gz}

# Print the number of lines in the compressed TSV file
echo $(zcat ${output} | wc -l)

# Print the count of unique genotype occurrences
echo $(zcat ${output} | cut -f 5 | sort | uniq -c)
