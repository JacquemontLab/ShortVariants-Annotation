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

# SQL database not required ?
# wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/loftee.sql.gz

# human ancestor not required ?
# wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/human_ancestor.fa.gz
# wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/human_ancestor.fa.gz.fai
# wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh38/human_ancestor.fa.gz.gzi




## For GRCh37 using loftee grch38 plugin
# Generate GERP DATABASE

# wget https://github.com/konradjk/loftee/archive/refs/tags/v1.0.4_GRCh38.tar.gz

# mkdir -p loftee-1.0.4_GRCh37
# tar -xvzf v1.0.4_GRCh38.tar.gz -C loftee-1.0.4_GRCh37

# mv loftee-1.0.4_GRCh37/loftee-1.0.4_GRCh38/* loftee-1.0.4_GRCh37/
# rm -rf loftee-1.0.4_GRCh37/loftee-1.0.4_GRCh38/

# cd loftee-1.0.4_GRCh37

# GERP database
# wget http://hgdownload.soe.ucsc.edu/gbdb/hg19/bbi/All_hg19_RS.bw -O gerp_conservation_scores.homo_sapiens.GRCh38.bw
# OR
# wget https://personal.broadinstitute.org/konradk/loftee_data/GRCh37/GERP_scores.final.sorted.txt.gz

# zcat GERP_scores.final.sorted.txt.gz \
# | awk 'BEGIN{OFS="\t"} !/^#/ {print "chr"$1, $2-1, $2, $3}' \
# > gerp.bedGraph

# sort -k1,1 -k2,2n gerp.bedGraph > gerp.sorted.bedGraph

# wget http://hgdownload.cse.ucsc.edu/goldenPath/hg19/bigZips/hg19.chrom.sizes

# module load kent_tools
# bigWigInfo gerp.hg19.bw
# bedGraphToBigWig gerp.sorted.bedGraph hg19.chrom.sizes gerp.hg19.bw





cd "$ORIG_DIR"