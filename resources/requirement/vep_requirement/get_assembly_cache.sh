#!/bin/bash
# Florian Bénitière 16/03/2025
# It downloads the required indexed cache files for both GRCh38 GRCh38 (hg38) assembly (command lines commented for GRCh37 (hg19)).

ORIG_DIR=$(pwd)

path_project=resources/vep_cache/
mkdir -p $path_project
cd $path_project

# Download cache assembly data for vep
curl -O https://ftp.ensembl.org/pub/release-113/variation/indexed_vep_cache/homo_sapiens_vep_113_GRCh38.tar.gz
tar xzf homo_sapiens_vep_113_GRCh38.tar.gz

# For GRCh37
# curl -O https://ftp.ensembl.org/pub/release-113/variation/indexed_vep_cache/homo_sapiens_vep_113_GRCh37.tar.gz
# tar xzf homo_sapiens_vep_113_GRCh37.tar.gz


cd "$ORIG_DIR"