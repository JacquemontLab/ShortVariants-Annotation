#!/bin/bash
# Download and prepare all reference resources and dockers required for variant annotation


set -e  # Exit immediately if a command exits with a non-zero status
set -o pipefail  # Properly propagate errors through pipelines

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
# Environment Resources Script
# ============================

echo "🔧 Creating Python virtual environment..."
bash resources/requirement/venv.sh

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