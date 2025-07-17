#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status
set -o pipefail  # Properly propagate errors through pipelines

# ============================
# Environment Setup Script
# ============================

echo "🔧 Creating Python virtual environment..."
bash setup/requirement/venv.sh

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