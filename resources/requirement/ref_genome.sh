#!/bin/bash
# Florian Bénitière 11/05/2026
# Download, bgzip-compress, and index GRCh38 reference genome for VEP pipelines
# Usage: ./ref_genome.sh <genome_version>
# Example: ./ref_genome.sh GRCh37

set -e
set -o pipefail

# ============================
# Check argument
# ============================
if [ $# -ne 1 ]; then
    echo "Usage: $0 <genome_version>"
    echo "Example: $0 GRCh37"
    exit 1
fi

GENOME_VERSION="$1"
if [[ "$GENOME_VERSION" != "GRCh37" && "$GENOME_VERSION" != "GRCh38" ]]; then
    echo "❌ Invalid genome version. Use 'GRCh37' or 'GRCh38'."
    exit 1
fi

echo "📥 Preparing reference genome for $GENOME_VERSION..."


# ============================
# Check dependencies
# ============================
for cmd in samtools bgzip; do
    if ! command -v $cmd &> /dev/null; then
        echo "❌ $cmd is not available"
    fi
done

echo "✅ samtools, bgzip are available"


# ============================
# Set URLs and filenames
# ============================
ORIG_DIR=$(pwd)
OUTDIR="resources/reference_genome/"
mkdir -p "$OUTDIR"

REF_FILENAME="ref_genome_${GENOME_VERSION}.fa"
GZ_FILENAME="${REF_FILENAME}.gz"

if [ "$GENOME_VERSION" == "GRCh38" ]; then
    URL="https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/GRCh38_reference_genome/GRCh38_full_analysis_set_plus_decoy_hla.fa"
elif [ "$GENOME_VERSION" == "GRCh37" ]; then
    URL="https://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/hg19.fa.gz"
fi

# ============================
# Download and prepare genome
# ============================
cd "$OUTDIR"

if [ "$GENOME_VERSION" == "GRCh38" ]; then
    wget -c "$URL"
    ORIGINAL_FILE=$(basename "$URL")  # GRCh38_full_analysis_set_plus_decoy_hla.fa
    mv "$ORIGINAL_FILE" "$REF_FILENAME"
    bgzip -f "$REF_FILENAME"
    samtools faidx "$GZ_FILENAME"
elif [ "$GENOME_VERSION" == "GRCh37" ]; then
    wget -c "$URL"
    gunzip -f hg19.fa.gz
    mv hg19.fa "$REF_FILENAME"
    bgzip -f "$REF_FILENAME"
    samtools faidx "$GZ_FILENAME"
fi

echo "✅ Reference genome $GENOME_VERSION prepared as $GZ_FILENAME in $OUTDIR"

cd "$ORIG_DIR"