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

echo "🧬 Downloading VEP cache... ⚠️ Needs to adapt the script because here you just want the .tar.gz uncompressed to be passed."
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


# Then the ShortVariants-Annotation/ directory need to be saved on the bucket/project

gsutil -m -u terra-user cp homo_sapiens_vep_113_GRCh38.tar.gz gs://fc-secure-BUCKET_ID/UPSTREAM_ANALYSIS/AllofUs_tierv8_ShortVariants_annot_intermediate/ShortVariants-Annotation/resources/vep_cache/homo_sapiens_vep_113_GRCh38.tar.gz

gsutil -m -u terra-user cp ressources_loftee.tar.gz gs://fc-secure-BUCKET_ID/UPSTREAM_ANALYSIS/AllofUs_tierv8_ShortVariants_annot_intermediate/ShortVariants-Annotation/resources/vep_cache/ressources_loftee.tar.gz

gsutil -m -u terra-user cp ressources_alphamissense.tar.gz gs://fc-secure-BUCKET_ID/UPSTREAM_ANALYSIS/AllofUs_tierv8_ShortVariants_annot_intermediate/ShortVariants-Annotation/resources/vep_cache/ressources_alphamissense.tar.gz

gsutil -m -u terra-user cp ressources_spliceai.tar.gz gs://fc-secure-BUCKET_ID/UPSTREAM_ANALYSIS/AllofUs_tierv8_ShortVariants_annot_intermediate/ShortVariants-Annotation/resources/vep_cache/ressources_spliceai.tar.gz

cd "$ORIG_DIR"