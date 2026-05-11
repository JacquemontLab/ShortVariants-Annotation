#!/bin/bash
# ================================================
# Script: get_loftee_resources.sh
# Author: Florian Bénitière
# Date: 11/05/2026
#
# Description:
#   Downloads and sets up the necessary LOFTEE resources for VEP pipelines.
#   Supports genome versions GRCh37 (hg19) and GRCh38 (hg38).
#   Downloads the LOFTEE repository and essential GERP conservation scores.
#
# Usage:
#   ./get_loftee_resources.sh <genome_version>
#   Example: ./get_loftee_resources.sh GRCh38
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

echo "📥 Downloading LOFTEE resources for $GENOME_VERSION"

# ============================
# Set paths
# ============================
ORIG_DIR=$(pwd)
BASE_DIR="resources/vep_cache/ressources_loftee"
mkdir -p "$BASE_DIR"
cd "$BASE_DIR"

# ============================
# Download LOFTEE repository
# ============================
if [ "$GENOME_VERSION" == "GRCh38" ]; then
    LOFTEE_TAR="v1.0.4_GRCh38.tar.gz"
elif [ "$GENOME_VERSION" == "GRCh37" ]; then
    # Use the GRCh38 plugin repository for GRCh37
    LOFTEE_TAR="v1.0.4_GRCh38.tar.gz"
fi

wget -c "https://github.com/konradjk/loftee/archive/refs/tags/$LOFTEE_TAR"
tar -xvzf "$LOFTEE_TAR"
rm "$LOFTEE_TAR"

# Rename extracted directory
if [ "$GENOME_VERSION" == "GRCh38" ]; then
    LOFTEE_DIR="loftee-1.0.4_GRCh38"
elif [ "$GENOME_VERSION" == "GRCh37" ]; then
    LOFTEE_DIR="loftee-1.0.4_GRCh37"
    mv loftee-1.0.4_GRCh38/ "$LOFTEE_DIR"
fi

cd "$LOFTEE_DIR"

# ============================
# Download GERP conservation scores
# ============================
if [ "$GENOME_VERSION" == "GRCh38" ]; then
    wget -c https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/gerp_conservation_scores.homo_sapiens.GRCh38.bw
elif [ "$GENOME_VERSION" == "GRCh37" ]; then
    wget http://hgdownload.soe.ucsc.edu/gbdb/hg19/bbi/All_hg19_RS.bw -O gerp_conservation_scores.homo_sapiens.GRCh38.bw
fi

echo "✅ LOFTEE resources for $GENOME_VERSION are ready in $BASE_DIR/$LOFTEE_DIR"

# Return to original directory
cd "$ORIG_DIR"



#### NOTES

# 1️⃣ SQL database
# The LOFTEE SQL database is optional. 
# Most pipelines do not require it:
# wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/loftee.sql.gz

# 2️⃣ Human ancestor reference
# Optional. Needed only if you require ancestral allele information in LOFTEE:
# wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/human_ancestor.fa.gz
# wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/human_ancestor.fa.gz.fai
# wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/human_ancestor.fa.gz.gzi

# 3️⃣ GERP conservation scores
# GRCh38: already downloaded as BigWig:
#   gerp_conservation_scores.homo_sapiens.GRCh38.bw

# GRCh37: BigWig is optional. You can either:
#   - Use UCSC precomputed BigWig:
#       wget http://hgdownload.soe.ucsc.edu/gbdb/hg19/bbi/All_hg19_RS.bw -O gerp_conservation_scores.homo_sapiens.GRCh38.bw
#   - Or generate from raw text GERP scores:
#       wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh37/GERP_scores.final.sorted.txt.gz
#       zcat GERP_scores.final.sorted.txt.gz \
#           | awk 'BEGIN{OFS="\t"} !/^#/ {print "chr"$1, $2-1, $2, $3}' \
#           > gerp.bedGraph
#       sort -k1,1 -k2,2n gerp.bedGraph > gerp.sorted.bedGraph
#       # Convert to BigWig (requires kent_tools):
#       wget http://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/hg19.chrom.sizes
#       bedGraphToBigWig gerp.sorted.bedGraph hg19.chrom.sizes gerp.hg19.bw