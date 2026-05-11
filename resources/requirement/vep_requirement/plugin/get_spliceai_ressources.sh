#!/bin/bash
# ================================================
# Script: get_spliceai_resources.sh
# Author: Florian Bénitière
# Date: 11/05/2026
#
# Description:
#   Downloads and sets up the necessary SpliceAI resources for VEP pipelines.
#   Supports genome versions GRCh37 (hg19) and GRCh38 (hg38).
#   Requires BaseSpace CLI for downloading Illumina resources.
#
# Usage:
#   ./get_spliceai_resources.sh <genome_version>
#   Example: ./get_spliceai_resources.sh GRCh38
# ================================================

set -e
set -o pipefail

# ============================
# Check argument
# ============================
if [ $# -ne 1 ]; then
    echo "Usage: $0 <genome_version>"
    echo "Example: $0 GRCh38"
    exit 1
fi

GENOME_VERSION="$1"
if [[ "$GENOME_VERSION" != "GRCh37" && "$GENOME_VERSION" != "GRCh38" ]]; then
    echo "❌ Invalid genome version. Use 'GRCh37' or 'GRCh38'."
    exit 1
fi

echo "📥 Downloading SpliceAI resources for $GENOME_VERSION"

# ============================
# Set paths
# ============================
ORIG_DIR=$(pwd)
BASE_DIR="resources/vep_cache"
SPLICEAI_DIR="$BASE_DIR/ressources_spliceai"
mkdir -p "$SPLICEAI_DIR"
cd "$BASE_DIR"

# ============================
# Download BaseSpace CLI
# ============================
echo "🔽 Downloading BaseSpace CLI..."
wget -q "https://launch.basespace.illumina.com/CLI/latest/amd64-linux/bs" -O bs
chmod +x bs

echo "🔑 Please authenticate BaseSpace CLI (this may open an interactive prompt)..."
./bs auth

# ============================
# Download SpliceAI VCFs
# ============================
if [ "$GENOME_VERSION" == "GRCh38" ]; then
    EXTENSIONS=("raw.snv.hg38.vcf.gz" "raw.indel.hg38.vcf.gz" \
                "raw.snv.hg38.vcf.gz.tbi" "raw.indel.hg38.vcf.gz.tbi")
elif [ "$GENOME_VERSION" == "GRCh37" ]; then
    EXTENSIONS=("raw.snv.hg19.vcf.gz" "raw.indel.hg19.vcf.gz" \
                "raw.snv.hg19.vcf.gz.tbi" "raw.indel.hg19.vcf.gz.tbi")
fi

for ext in "${EXTENSIONS[@]}"; do
    echo "🔽 Downloading $ext ..."
    ./bs download project -i 66029966 --extension="$ext"
done

# Move downloaded files to SpliceAI resources folder
mv genome_scores_v1.3_ds.20a701bc58ab45b59de2576db79ac8d0 "$SPLICEAI_DIR"

# Cleanup BaseSpace CLI
rm bs

echo "✅ SpliceAI resources for $GENOME_VERSION are ready in $SPLICEAI_DIR"

# Return to original directory
cd "$ORIG_DIR"