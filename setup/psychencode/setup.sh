#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status
set -o pipefail  # Properly propagate errors through pipelines

# ============================
# Environment Resources Script
# ============================

echo "🔧 Creating Python virtual environment..."
bash resources/requirement/venv.sh

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