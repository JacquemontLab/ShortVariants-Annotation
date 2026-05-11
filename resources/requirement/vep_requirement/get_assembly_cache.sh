#!/bin/bash
# ================================================
# Script: get_assembly_cache.sh
# Author: Florian Bénitière
# Date: 11/05/2026
#
# Description:
#   Download the indexed VEP cache files for the specified genome assembly.
#   Supports GRCh37 (hg19) and GRCh38 (hg38).
#
# Usage:
#   ./get_assembly_cache.sh <genome_version>
#   Example: ./get_assembly_cache.sh GRCh38
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

echo "📥 Downloading VEP cache for $GENOME_VERSION"

# ============================
# Set paths and URLs
# ============================
ORIG_DIR=$(pwd)
CACHE_DIR="resources/vep_cache"
mkdir -p "$CACHE_DIR"
cd "$CACHE_DIR"

# Determine correct URL
if [ "$GENOME_VERSION" == "GRCh38" ]; then
    VEP_CACHE_URL="https://ftp.ensembl.org/pub/release-113/variation/indexed_vep_cache/homo_sapiens_vep_113_GRCh38.tar.gz"
elif [ "$GENOME_VERSION" == "GRCh37" ]; then
    VEP_CACHE_URL="https://ftp.ensembl.org/pub/release-113/variation/indexed_vep_cache/homo_sapiens_vep_113_GRCh37.tar.gz"
fi

# ============================
# Download and extract
# ============================
CACHE_TAR=$(basename "$VEP_CACHE_URL")

curl -O "$VEP_CACHE_URL"
tar xzf "$CACHE_TAR"

echo "✅ VEP cache for $GENOME_VERSION downloaded and extracted to $CACHE_DIR"

# Return to original directory
cd "$ORIG_DIR"