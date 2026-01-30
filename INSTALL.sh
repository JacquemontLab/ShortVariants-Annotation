#!/bin/bash
# Download and prepare all reference resources required for variant annotation


set -e  # Exit immediately if a command exits with a non-zero status
set -o pipefail  # Properly propagate errors through pipelines

# ============================
# Check dependencies
# ============================

if ! command -v samtools >/dev/null 2>&1; then
    echo "❌ samtools is not installed. Install via: sudo dnf install samtools"
    exit 1
fi

if ! command -v tabix >/dev/null 2>&1; then
    echo "❌ tabix is not installed. Install via: sudo dnf install htslib"
    exit 1
fi

if ! command -v bgzip >/dev/null 2>&1; then
    echo "❌ bgzip is not installed. Install via: sudo dnf install htslib"
    exit 1
fi

echo "✅ samtools, tabix, and bgzip are available"

# ============================
# Environment Resources Script
# ============================
ORIG_DIR=$(pwd)

echo "🧬 Downloading reference genome... (requires samtools and htslib)"
bash resources/requirement/ref_genome.sh  # ⚠️ Ensure compatibility with your dataset

echo "🧬 Downloading VEP cache... (requires apptainer)"
bash resources/requirement/vep_requirement/get_assembly_cache.sh

echo "🧠 Downloading AlphaMissense resources... (requires tabix)"
bash resources/requirement/vep_requirement/plugin/get_alphamissense_ressources.sh

echo "💥 Downloading LoFTEE resources..."
bash resources/requirement/vep_requirement/plugin/get_loftee_ressources.sh

echo "🧬 Downloading SpliceAI resources..."
bash resources/requirement/vep_requirement/plugin/get_spliceai_ressources.sh

cd "$ORIG_DIR"