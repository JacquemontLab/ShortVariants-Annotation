#!/bin/bash
# Download and prepare all reference resources and dockers required for variant annotation


set -e  # Exit immediately if a command exits with a non-zero status
set -o pipefail  # Properly propagate errors through pipelines

# ============================
# Environment Resources Script
# ============================

echo "🔧 Creating Python virtual environment..."
bash resources/requirement/venv.sh

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


# ============================
# VEP Docker Requirements
# ============================

ORIG_DIR=$(pwd)


path_project=resources/vep_cache/
mkdir -p $path_project
cd $path_project

if ! command -v apptainer &> /dev/null; then
    echo "apptainer not found — loading module..."
    module load apptainer
else
    echo "apptainer already available."
fi

# Get VEP docker
apptainer pull ensembl-vep.sif docker://ensemblorg/ensembl-vep

cd "$ORIG_DIR"

# ============================
# System Requirements
# ============================

echo "📦 Please ensure the following tools are installed and available in your PATH:"
echo "  - apptainer"
echo "  - bcftools"
echo "  - vcftools"
echo ""
echo "🧪 You can check with:"
echo "  which apptainer"
echo "  which bcftools"
echo "  which vcftools"

echo "✅ Resources completed successfully."