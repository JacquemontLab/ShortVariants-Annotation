#!/bin/bash
# ================================================
# Script: get_alphamissense_resources.sh
# Author: Florian Bénitière
# Date: 11/05/2026
#
# Description:
#   Downloads and indexes AlphaMissense annotation files for VEP.
#   Supports genome versions GRCh37 (hg19) and GRCh38 (hg38).
#   Indexes files with tabix.
#
# Usage:
#   ./get_alphamissense_resources.sh <genome_version>
#   Example: ./get_alphamissense_resources.sh GRCh38
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

echo "📥 Downloading AlphaMissense resources for $GENOME_VERSION"

# ============================
# Check tabix
# ============================
if ! command -v tabix >/dev/null 2>&1; then
    echo "❌ tabix not available."
    exit 1
else
    echo "✅ tabix is available"
fi

# ============================
# Set paths
# ============================
ORIG_DIR=$(pwd)
BASE_DIR="resources/vep_cache/ressources_alphamissense"
mkdir -p "$BASE_DIR"
cd "$BASE_DIR"

# ============================
# Download and index AlphaMissense
# ============================
if [ "$GENOME_VERSION" == "GRCh38" ]; then
    FILE="AlphaMissense_hg38.tsv.gz"
    URL="https://storage.googleapis.com/dm_alphamissense/$FILE"
elif [ "$GENOME_VERSION" == "GRCh37" ]; then
    FILE="AlphaMissense_hg19.tsv.gz"
    URL="https://storage.googleapis.com/dm_alphamissense/$FILE"
fi

echo "🔽 Downloading $FILE..."
wget -c "$URL"

echo "🔧 Indexing $FILE with tabix..."
tabix -s 1 -b 2 -e 2 -f -S 1 "$FILE"

echo "✅ AlphaMissense resources for $GENOME_VERSION are ready in $BASE_DIR"

# Return to original directory
cd "$ORIG_DIR"