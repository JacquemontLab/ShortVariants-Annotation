#!/bin/bash
# Florian Bénitière 16/03/2025
# This script sets up the Ensembl Variant Effect Predictor (VEP) environment using Apptainer.

# It pulls the VEP Docker image and downloads the required indexed cache files for both GRCh38 and GRCh37 genome assemblies.
# Ensure you have Apptainer installed and internet access before running this script.


# Check if apptainer is available
if ! command -v apptainer &> /dev/null; then
    echo "apptainer not found — loading module..."
    module load apptainer
else
    echo "apptainer already available."
fi


ORIG_DIR=$(pwd)


path_project=resources/vep_cache/
mkdir -p $path_project
cd $path_project

# Get VEP docker
module load apptainer
apptainer pull ensembl-vep.sif docker://ensemblorg/ensembl-vep

# Download cache assembly data for vep
mkdir .vep_ressources
cd .vep_ressources

curl -O https://ftp.ensembl.org/pub/release-113/variation/indexed_vep_cache/homo_sapiens_vep_113_GRCh38.tar.gz
tar xzf homo_sapiens_vep_113_GRCh38.tar.gz
curl -O https://ftp.ensembl.org/pub/release-113/variation/indexed_vep_cache/homo_sapiens_vep_113_GRCh37.tar.gz
tar xzf homo_sapiens_vep_113_GRCh37.tar.gz


cd "$ORIG_DIR"