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


cd ShortVariants-Annotation/resources/vep_cache/

tar -czvf ressources_alphamissense.tar.gz ressources_alphamissense
tar -czvf ressources_loftee.tar.gz ressources_loftee
tar -czvf ressources_spliceai.tar.gz ressources_spliceai
cd "$ORIG_DIR"

# ============================
# Dockers Requirements
# ============================
mkdir -p ShortVariants-Annotation/resources/dockers/

docker pull ensemblorg/ensembl-vep:release_113.3
docker save ensemblorg/ensembl-vep:release_113.3 -o ShortVariants-Annotation/resources/dockers/ensembl-vep_113.3.tar

docker pull ghcr.io/jacquemontlab/genomics_tools:latest
docker save ghcr.io/jacquemontlab/genomics_tools:latest -o ShortVariants-Annotation/resources/dockers/genomics_tools.tar

# Then the ShortVariants-Annotation/ directory need to be saved on the bucket/project