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
bash setup/requirement/vep_requirement/get_alphamissense_ressources.sh

echo "💥 Downloading LoFTEE resources..."
bash setup/requirement/vep_requirement/get_loftee_ressources.sh

echo "🧬 Downloading SpliceAI resources..."
bash setup/requirement/vep_requirement/get_spliceai_ressources.sh


cd SNV-Annotation/resources/vep_cache/

tar -czvf ressources_alphamissense.tar.gz ressources_alphamissense
tar -czvf ressources_loftee.tar.gz ressources_loftee
tar -czvf ressources_spliceai.tar.gz ressources_spliceai
cd "$ORIG_DIR"

# ============================
# Dockers Requirements
# ============================
mkdir -p SNV-Annotation/resources/dockers/

docker pull ensemblorg/ensembl-vep:release_113.3
docker save ensemblorg/ensembl-vep:release_113.3 -o SNV-Annotation/resources/dockers/ensembl-vep_113.3.tar

docker pull flobenhsj/genomics-tools_v1.0:latest
docker save flobenhsj/genomics-tools_v1.0:latest -o SNV-Annotation/resources/dockers/genomics-tools_v1.0.tar

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

echo "✅ Setup completed successfully."

# Then the SNV-Annotation/ directory need to be saved on the bucket/project