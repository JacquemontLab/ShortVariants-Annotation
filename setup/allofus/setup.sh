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


# Then the ShortVariants-Annotation/ directory need to be saved on the bucket/project

gsutil -m -u terra-user cp homo_sapiens_vep_113_GRCh38.tar.gz gs://fc-secure-BUCKET_ID/UPSTREAM_ANALYSIS/AllofUs_tierv8_ShortVariants_annot_intermediate/ShortVariants-Annotation/resources/vep_cache/homo_sapiens_vep_113_GRCh38.tar.gz

gsutil -m -u terra-user cp ressources_loftee.tar.gz gs://fc-secure-BUCKET_ID/UPSTREAM_ANALYSIS/AllofUs_tierv8_ShortVariants_annot_intermediate/ShortVariants-Annotation/resources/vep_cache/ressources_loftee.tar.gz

gsutil -m -u terra-user cp ressources_alphamissense.tar.gz gs://fc-secure-BUCKET_ID/UPSTREAM_ANALYSIS/AllofUs_tierv8_ShortVariants_annot_intermediate/ShortVariants-Annotation/resources/vep_cache/ressources_alphamissense.tar.gz

gsutil -m -u terra-user cp ressources_spliceai.tar.gz gs://fc-secure-BUCKET_ID/UPSTREAM_ANALYSIS/AllofUs_tierv8_ShortVariants_annot_intermediate/ShortVariants-Annotation/resources/vep_cache/ressources_spliceai.tar.gz

cd "$ORIG_DIR"