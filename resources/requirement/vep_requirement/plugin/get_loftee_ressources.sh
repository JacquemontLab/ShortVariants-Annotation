#!/bin/bash
# Florian Bénitière 16/03/2025
# This script downloads and sets up the necessary resources for running LOFTEE with VEP.

# It creates a dedicated directory for LOFTEE resources, downloads the LOFTEE repository,
# and fetches essential files, including the GERP conservation scores, SQL database, 
# and human ancestor reference for the GRCh38 genome assembly.
# Ensure you have internet access before running this script.

ORIG_DIR=$(pwd)

path_project=resources/vep_cache/
mkdir -p $path_project/ressources_loftee
cd $path_project/ressources_loftee


# Download files to run LOFTEE
# Git repository
wget https://github.com/konradjk/loftee/archive/refs/tags/v1.0.4_GRCh38.tar.gz

tar -xvzf v1.0.4_GRCh38.tar.gz
rm v1.0.4_GRCh38.tar.gz

cd loftee-1.0.4_GRCh38

# GERP database
wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/gerp_conservation_scores.homo_sapiens.GRCh38.bw

# SQL database
wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/loftee.sql.gz

# human ancestor
wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/human_ancestor.fa.gz
wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/human_ancestor.fa.gz.fai
wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/human_ancestor.fa.gz.gzi



cd "$ORIG_DIR"