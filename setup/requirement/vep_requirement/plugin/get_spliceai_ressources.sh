#!/bin/bash
# Florian Bénitière 16/03/2025
# This script downloads and sets up the necessary resources for running SpliceAI with VEP.

# It creates a directory for SpliceAI resources, downloads the BaseSpace CLI from Illumina,
# and fetches SpliceAI VCF files for both GRCh38 and GRCh19 genome assemblies.
# Ensure you have internet access and the necessary permissions before running this script.

ORIG_DIR=$(pwd)

path_project=resources/vep_cache/
mkdir -p $path_project/ressources_spliceai
cd $path_project

# Download cache assembly data for vep


# Download files to run spliceAI from BaseSpace Illumina

echo "🔽 Downloading BaseSpace CLI..."
wget -q "https://launch.basespace.illumina.com/CLI/latest/amd64-linux/bs" -O bs
chmod +x bs

echo "🔑 Please authenticate BaseSpace CLI (this may open an interactive prompt)..."
./bs auth

./bs download project -i 66029966 --extension=raw.snv.hg38.vcf.gz
./bs download project -i 66029966 --extension=raw.indel.hg38.vcf.gz
./bs download project -i 66029966 --extension=raw.snv.hg38.vcf.gz.tbi
./bs download project -i 66029966 --extension=raw.indel.hg38.vcf.gz.tbi



./bs download project -i 66029966 --extension=raw.snv.hg19.vcf.gz
./bs download project -i 66029966 --extension=raw.indel.hg19.vcf.gz
./bs download project -i 66029966 --extension=raw.snv.hg19.vcf.gz.tbi
./bs download project -i 66029966 --extension=raw.indel.hg19.vcf.gz.tbi

mv genome_scores_v1.3_ds.20a701bc58ab45b59de2576db79ac8d0 ressources_spliceai

rm bs


cd "$ORIG_DIR"