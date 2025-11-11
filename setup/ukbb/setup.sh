#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status
set -o pipefail  # Properly propagate errors through pipelines

# ============================
# Environment Setup Script
# ============================
ORIG_DIR=$(pwd)

echo "🧬 Downloading reference genome... (requires samtools and htslib)"
bash setup/requirement/ref_genome.sh  # ⚠️ Ensure compatibility with your dataset

echo "🧬 Downloading VEP cache... (requires apptainer)"
bash setup/requirement/vep_requirement/get_vep_docker.sh

echo "🧠 Downloading AlphaMissense resources... (requires tabix)"
bash setup/requirement/vep_requirement/plugin/get_alphamissense_ressources.sh

echo "💥 Downloading LoFTEE resources..."
bash setup/requirement/vep_requirement/plugin/get_loftee_ressources.sh

echo "🧬 Downloading SpliceAI resources..."
bash setup/requirement/vep_requirement/plugin/get_spliceai_ressources.sh


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

docker pull flobenhsj/genomics-tools_v1.0:latest
docker save flobenhsj/genomics-tools_v1.0:latest -o ShortVariants-Annotation/resources/dockers/genomics-tools_v1.0.tar


# Then the ShortVariants-Annotation/ directory need to be saved on the bucket/project