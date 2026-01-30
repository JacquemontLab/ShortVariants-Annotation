#!/bin/bash
# Florian Bénitière 16/03/2025
# Download, bgzip-compress, and index GRCh38 reference genome for VEP pipelines

# Check if htslib is available
if ! command -v htslib &> /dev/null; then
    echo "htslib not found — loading module..."
    module load htslib
else
    echo "htslib already available."
fi

# Check if samtools is available
if ! command -v htslib &> /dev/null; then
    echo "samtools not found — loading module..."
    module load samtools
else
    echo "samtools already available."
fi


ORIG_DIR=$(pwd)

# Define variables
URL="https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/GRCh38_reference_genome/GRCh38_full_analysis_set_plus_decoy_hla.fa"
OUTDIR="resources/reference_genome/"  # 🔁 Replace with your desired output directory

FILENAME=$(basename "$URL")  # GRCh38_full_analysis_set_plus_decoy_hla.fa
GZ_FILENAME="${FILENAME}.gz"  # GRCh38_full_analysis_set_plus_decoy_hla.fa.gz

# Create output directory if needed
mkdir -p "$OUTDIR"

# Download the FASTA file
wget -c "$URL" -P "$OUTDIR"

# Change to output directory
cd "$OUTDIR"

# Compress the FASTA file
bgzip -f "$FILENAME"  # `-f` overwrites if the file already exists

# Index with samtools
samtools faidx "$GZ_FILENAME"

cd "$ORIG_DIR"