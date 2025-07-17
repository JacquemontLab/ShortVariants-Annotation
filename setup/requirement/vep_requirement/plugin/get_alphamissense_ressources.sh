#!/bin/bash
# Florian Bénitière 16/03/2025
# This script downloads and indexes AlphaMissense annotation files for VEP.

# It creates a dedicated directory for resources, fetches the necessary data 
# for both GRCh37 (hg19) and GRCh38 (hg38) assemblies, and indexes them using tabix.
# Ensure that 'tabix' is available in your environment before running this script.


# Check if tabix is available
if ! command -v tabix &> /dev/null; then
    echo "tabix not found — loading module..."
    module load tabix
else
    echo "tabix already available."
fi

ORIG_DIR=$(pwd)

path_project=resources/vep_cache/
mkdir -p $path_project/ressources_alphamissense
cd $path_project/ressources_alphamissense

# Download cache assembly data for vep

# Download files to run AlphaMissense

wget https://storage.googleapis.com/dm_alphamissense/AlphaMissense_hg38.tsv.gz
tabix -s 1 -b 2 -e 2 -f -S 1 AlphaMissense_hg38.tsv.gz

wget https://storage.googleapis.com/dm_alphamissense/AlphaMissense_hg19.tsv.gz
tabix -s 1 -b 2 -e 2 -f -S 1 AlphaMissense_hg19.tsv.gz


cd "$ORIG_DIR"