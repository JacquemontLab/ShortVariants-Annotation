
#!/bin/bash

# Usage: ./process_batch.sh list_sample_to_process.txt input_file_gvcf_path fasta_ref cpus

# set -euo pipefail

# Arguments
list_sample_to_process="$1"
input_file_gvcf_path="$2"
fasta_ref="$3"
cpus="$4"
batch_id=$(basename "${list_sample_to_process}")

mkdir -p data/processed/gvcf_per_sample/${batch_id}/

process_sample() {
    pwd 
    sample="$1"
    input_gvcf=$(zgrep -P "${sample}\t" "${input_file_gvcf_path}" | cut -f2)

    echo "${input_gvcf}"

    # Download from DNA Nexus storage the gVCF
    dx download "${input_gvcf}" -o ${sample}.vcf.gz
    dx download "${input_gvcf}.tbi" -o ${sample}.vcf.gz.tbi

    ./extraction_snps_indels_UKBB.sh \
        "${sample}" \
        "${sample}.vcf.gz" \
        "data/processed/gvcf_per_sample/${batch_id}/${sample}.tsv.gz" \
        "${fasta_ref}" \
        "${cpus}"
}

export -f process_sample
export input_file_gvcf_path fasta_ref batch_id

# Use GNU parallel to run multiple samples in parallel
cat "${list_sample_to_process}" | parallel -j "${cpus}" process_sample {}

# Mark the batch as complete
mkdir -p data/processed/step/
touch data/processed/step/${batch_id}_gvcf_produced