#!/bin/bash
# Florian Bénitière 11/05/2026
# Download and prepare all reference resources required for variant annotation
# Supports GRCh37 and GRCh38 genome versions

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

echo "Using genome version: $GENOME_VERSION"

# ============================
# Check dependencies
# ============================
for cmd in samtools tabix bgzip; do
    if ! command -v $cmd >/dev/null 2>&1; then
        echo "❌ $cmd is not available"
        exit 1
    fi
done
echo "✅ samtools, tabix, and bgzip are available"

# ============================
# Environment Resources Script
# ============================
ORIG_DIR=$(pwd)

echo "🧬 Downloading reference genome..."
bash resources/requirement/ref_genome.sh "$GENOME_VERSION"

echo "🧬 Downloading VEP cache..."
bash resources/requirement/vep_requirement/get_assembly_cache.sh "$GENOME_VERSION"

echo "🧠 Downloading AlphaMissense resources..."
bash resources/requirement/vep_requirement/plugin/get_alphamissense_ressources.sh "$GENOME_VERSION"

echo "💥 Downloading LoFTEE resources..."
bash resources/requirement/vep_requirement/plugin/get_loftee_ressources.sh "$GENOME_VERSION"

echo "🧬 Downloading SpliceAI resources..."
bash resources/requirement/vep_requirement/plugin/get_spliceai_ressources.sh "$GENOME_VERSION"

cd "$ORIG_DIR"