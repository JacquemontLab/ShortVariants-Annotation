version 1.1

workflow process_gvcf_by_batch {
  String project = "project-XXXXXXXX"

  input {
    File sample_list                  # One sample ID per line
    File file_gvcf_path               # 2-column TSV: sampleid<TAB>path_to_gvcf
    Int num_batches                   # e.g., 10
    Int count_start                   # starting indexes of batch, to be able to run WDL by batch...
  }

  call SplitSampleList {
    input:
      sample_list = sample_list,
      num_batches = num_batches,
      count_start = count_start
  }

## So here it goes nuts... I'm trying to run not more than 6 batches at a time, since the bandwith will saturate otherwise. There is no other 
## way in WDL for me that having different scatter function...
  scatter (batch_file in SplitSampleList.batch_files) {
    call ProduceTSVPerSampleUKBB {
      input:
        list_sample_to_process = batch_file,
        file_gvcf_path = file_gvcf_path,
        project = project
    }
  }

  scatter (archive in ProduceTSVPerSampleUKBB.output_archive) {
    call MergeTSVParquetByBatch {
      input:
        archive = archive
    }
  }

  call MergeBatches {
    input:
      batches_files = MergeTSVParquetByBatch.parquet_compressed
  }

  call FindUniqSNVsVCF {
    input:
      parquet_including_all_snv = MergeBatches.unannotated_parquet_compressed
  }

  # VEP Default Plugin
  scatter (vcf_to_annot in FindUniqSNVsVCF.vcf_tarballs) {
    call RunVEPDefault {
      input:
        vcf_to_annot = vcf_to_annot,
        project = project
    }
  }

  call ConvertVEPOutParquet as ConvertVEPOutDefault {
    input:
      list_tsv = RunVEPDefault.tsv_vep,
      plugin = "default"
  }

  # VEP SpliceAI Plugin
  scatter (vcf_to_annot in FindUniqSNVsVCF.vcf_tarballs) {
    call RunVEPSpliceAI {
      input:
        vcf_to_annot = vcf_to_annot,
        project = project
    }
  }

  call ConvertVEPOutParquet as ConvertVEPOutSpliceAI {
    input:
      list_tsv = RunVEPSpliceAI.tsv_vep,
      plugin = "spliceai"
  }

  # VEP Alphamissense Plugin
  scatter (vcf_to_annot in FindUniqSNVsVCF.vcf_tarballs) {
    call RunVEPAlphamissense {
      input:
        vcf_to_annot = vcf_to_annot,
        project = project
    }
  }

  call ConvertVEPOutParquet as ConvertVEPOutAlphamissense {
    input:
      list_tsv = RunVEPAlphamissense.tsv_vep,
      plugin = "alphamissense"
  }

  # VEP LOFTEE Plugin
  scatter (vcf_to_annot in FindUniqSNVsVCF.vcf_tarballs) {
    call RunVEPLoftee {
      input:
        vcf_to_annot = vcf_to_annot,
        project = project
    }
  }

  call ConvertVEPOutParquet as ConvertVEPOutLoftee {
    input:
      list_tsv = RunVEPLoftee.tsv_vep,
      plugin = "loftee"
  }

  call LossLessAnnotation {
    input:
      plugins_parquet = [
        ConvertVEPOutSpliceAI.plugin_parquet_compressed,
        ConvertVEPOutAlphamissense.plugin_parquet_compressed,
        ConvertVEPOutLoftee.plugin_parquet_compressed
      ],
      vep_default_parquet = ConvertVEPOutDefault.plugin_parquet_compressed,
      parquet_including_all_snv = MergeBatches.unannotated_parquet_compressed
  }

  call RefinedAnnotation {
    input:
      lossless_parquet = LossLessAnnotation.lossless_parquet_compressed
  }

  output {
    Array[File] batch_done_flags = ProduceTSVPerSampleUKBB.output_archive
    Array[File] batch_files = SplitSampleList.batch_files
    Array[File] parquet_archives = MergeTSVParquetByBatch.parquet_compressed
    File final_merged_parquet = MergeBatches.unannotated_parquet_compressed
    Array[File] vcf_tarballs = FindUniqSNVsVCF.vcf_tarballs
    Array[File] vep_output_txt = RunVEPDefault.stat
    Array[File] vep_output_html = RunVEPDefault.stat_html
    Array[File] vep_output_tsv = RunVEPDefault.tsv_vep
    File refined_parquet = RefinedAnnotation.refined_parquet
    File summary_report = RefinedAnnotation.summary_report
    File lossless_parquet = LossLessAnnotation.lossless_parquet_compressed
    File lossless_annotation_log = LossLessAnnotation.log_output
  }
}



task SplitSampleList {
    # This rule splits a large sample list into multiple smaller files (batches) 
    # to facilitate parallel processing. Each batch contains approximately equal 
    # numbers of lines, ensuring balanced workload distribution.
  input {
    File sample_list
    Int num_batches
    Int count_start  # Starting index
    String output_prefix = "data/inputs/batches/"
  }

  command <<<
    mkdir -p "~{output_prefix}"

        # Count total lines 
    total_lines=$(wc -l < "~{sample_list}")

        # Determine number of lines per batch
    lines_per_batch=$(( (total_lines + ~{num_batches} - 1) / ~{num_batches} ))

        # Split the file into num_batches files
    split -d -a 4 -l $lines_per_batch "~{sample_list}" "~{output_prefix}batch_"

    # Rename files to start numbering at count_start
    files=( $(ls -v ~{output_prefix}batch_*) )
    total_files=${#files[@]}

    for (( i=total_files-1; i>=0; i-- )); do
      new_index=$((~{count_start} + i))
      mv "${files[i]}" "~{output_prefix}batch_$(printf '%04d' $new_index)"
    done

    ls "~{output_prefix}"
  >>>

  output {
    Array[File] batch_files = glob("~{output_prefix}batch_*")
  }

  runtime {
    docker: "ubuntu:latest"
    dx_instance_type: "mem1_ssd1_v2_x4"    
  }
}




task ProduceTSVPerSampleUKBB {
    # This rule processes per-sample gVCF files for the UKBB dataset for a given batch.
    # It extracts SNPs, INDELs, and Non Homozygous reference sites from variant calls.
    # The processed data is saved as a compressed TSV file for each sample.
  input {
    File list_sample_to_process      # file with 1 sample ID per line
    File file_gvcf_path              # 2-column: sample<TAB>gvcf_path
    Int cpu = 72
    String project
    
  }

  # Derive batch_id from input file name (for use in output section)
  String batch_id = basename(list_sample_to_process)

  command <<<
    # Download scripts and Docker image
    dx download "~{project}:SNV-Annotation/resources/dockers/genomics-tools_v1.0.tar"
    dx download "~{project}:SNV-Annotation/scripts/dataset_specific/extraction_snps_indels_UKBB.sh"
    dx download "~{project}:SNV-Annotation/scripts/dataset_specific/produce_tsv_per_sample_UKBB.sh"
    
    # Download reference genome
    dx download "~{project}:SNV-Annotation/resources/reference_genome/GRCh38_full_analysis_set_plus_decoy_hla.fa.gz"
    dx download "~{project}:SNV-Annotation/resources/reference_genome/GRCh38_full_analysis_set_plus_decoy_hla.fa.gz.gzi"
    fasta_ref="GRCh38_full_analysis_set_plus_decoy_hla.fa.gz"

    # Load Docker image
    docker load -i genomics-tools_v1.0.tar

    # Pass DNAnexus environment variables to container
    env | grep '^DX_' > dx_env.list

    # Make sure downloaded shell scripts are executable.
    chmod +x *

    # Run the pipeline inside the container
    docker run --env-file dx_env.list --rm -v /home/dnanexus/:/home/dnanexus/ -v $(pwd):/data -w /data flobenhsj/genomics-tools_v1.0:latest bash -c "
      bash ./produce_tsv_per_sample_UKBB.sh \
        '~{list_sample_to_process}' \
        '~{file_gvcf_path}' \
        '${fasta_ref}' \
        '~{cpu}'
    " 2>&1 | tee output.log


    # Compress the directory containing per-sample TSVs using pigz for speed.
    # Install pigz inside the container
    apt-get update && apt-get install -y pigz
    tar --use-compress-program=pigz -cf ~{batch_id}_gvcf.tar.gz -C data/processed/gvcf_per_sample/~{batch_id} .
  >>>

  output {
    File output_archive = "~{batch_id}_gvcf.tar.gz"
  }

  runtime {
    dx_instance_type: "mem1_ssd1_v2_x72"
  }
}


task MergeTSVParquetByBatch {
    # This rule merges multiple TSV files containing gVCF data into a single Parquet file for a given batch.
    # Note : 1sec per samples, PySpark is able to multithread but it might not necessarily changed the process time
  input {
    File archive            # Archive containing gvcf TSV formated
    Int cpu = 16             # default to 16 CPUs
    Int mem_per_cpu = 2     # default to 2 GB per CPU
  }

  # Derive batch_id by removing known suffix from filename
  String batch_id = basename(archive, "_gvcf.tar.gz")

  command <<<
    set -euo pipefail

    # Extract the archive contents (TSV files) into the new directory.
    mkdir -p Archive_gvcf
    tar --use-compress-program=pigz -xf ~{archive} -C Archive_gvcf

    # Run the PySpark script to merge TSV files into one Parquet file:
    driver_memory=$(awk "BEGIN {printf \"%d\", ~{cpu} * ~{mem_per_cpu}}")
    /opt/spark/bin/spark-submit --driver-memory "$driver_memory"g /usr/bin/generate_parquet_all_snvs.py "Archive_gvcf" "~{batch_id}.parquet" ~{cpu} ~{mem_per_cpu} 2>&1 | tee output.log

    # Compress the resulting Parquet file using pigz.
    tar --use-compress-program=pigz -cf "~{batch_id}.parquet.tar.gz" "~{batch_id}.parquet"

  >>>


  output {
    File parquet_compressed = "~{batch_id}.parquet.tar.gz"
  }

  runtime {
    docker: "flobenhsj/spark-tsv-to-parquet_v1.0:latest"
    dx_instance_type: "mem1_ssd1_v2_x16"
  }
}


task MergeBatches {
    # This rule merges multiple batch-level Parquet files into a single consolidated Parquet file.
    # Note : 1sec per samples, PySpark is able to multithread but it might not necessarily changed the process time
  input {
    Array[File] batches_files             # list of .parquet.tar.gz files
    Int cpu = 96             # default to 96 CPUs
    Int mem_per_cpu = 8     # default to 4 GB per CPU
  }


  command <<<
    set -euo pipefail

    # Unpack each archive into the uncompressed_parquets/ directory
    mkdir -p uncompressed_parquets
    for archive in ~{sep=' ' batches_files}; do
      tar --use-compress-program=pigz -xf "$archive" -C uncompressed_parquets
    done

    # Generate a comma-separated list of all .parquet files
    parquet_files=$(find uncompressed_parquets -maxdepth 1 -name "*.parquet" | paste -sd, -)

    echo "Merging the following parquet files:"
    echo "$parquet_files"

    # Launch the PySpark script to merge all the found .parquet files into one
    driver_memory=$(awk "BEGIN {printf \"%d\", ~{cpu} * ~{mem_per_cpu}}")
    /opt/spark/bin/spark-submit --driver-memory "$driver_memory"g /usr/bin/merge_parquets.py "$parquet_files" "Unannotated_SNVs.parquet" ~{cpu} ~{mem_per_cpu} 2>&1 | tee output.log

    # Compress the merged output parquet file using pigz
    tar --use-compress-program=pigz -cf "Unannotated_SNVs.parquet.tar.gz" "Unannotated_SNVs.parquet"
  >>>


  output {
    File unannotated_parquet_compressed = "Unannotated_SNVs.parquet.tar.gz"
  }

  runtime {
    docker: "flobenhsj/spark-tsv-to-parquet_v1.0:latest"
    dx_instance_type: "mem3_ssd3_x96"
  }
}





task FindUniqSNVsVCF {
    # This rule extracts unique SNVs from a large Parquet file and generates chromosome-specific VCF files
  input {
    File parquet_including_all_snv             # list of .parquet.tar.gz files
    Int cpu = 96             # default to 96 CPUs
    Int mem_per_cpu = 4     # default to 4 GB per CPU
  }

  # Derive file name by removing known suffix from filename
  String parquet_name = basename(parquet_including_all_snv, ".tar.gz")

  command <<<
    set -euo pipefail

    # Decompress the input archive (~{parquet_including_all_snv}) using pigz
    tar --use-compress-program=pigz -xf ~{parquet_including_all_snv}

    # Run PySpark script to generate per-chromosome VCFs of unique SNVs from the input Parquet
    mkdir -p "data/processed/vcf_uniq_snvs/"
    driver_memory=$(awk "BEGIN {printf \"%d\", ~{cpu} * ~{mem_per_cpu}}")
    /opt/spark/bin/spark-submit --driver-memory "$driver_memory"g /usr/bin/generate_parquet_uniq_snvs.py ~{parquet_name} "data/processed/vcf_uniq_snvs/" ~{cpu} ~{mem_per_cpu} 2>&1 | tee output.log

    # Post-process VCF files: sort, compress, index
    for file in data/processed/vcf_uniq_snvs/chr*.vcf; do

      chrom="${file%.vcf}"
      vcf-sort "$file" > "${chrom}_sorted.vcf"
      
      mv "${chrom}_sorted.vcf" "$file"

      bgzip -c "$file" > "$file.gz"
      tabix -p vcf "$file.gz"

      # Bundle only filenames (not full paths)
      tar --use-compress-program=pigz -cf "${chrom}.vcf_bundle.tar.gz" -C "$(dirname "$file")" \
          "$(basename "$file.gz")" "$(basename "$file.gz.tbi")"
    done
  >>>


  output {
    Array[File] vcf_tarballs = glob("data/processed/vcf_uniq_snvs/*.vcf_bundle.tar.gz")
  }

  runtime {
    docker: "flobenhsj/spark-tsv-to-parquet_v1.0:latest"
    dx_instance_type: "mem2_ssd2_v2_x96"
  }
  
}




task RunVEPDefault {
    # This rule runs the Ensembl Variant Effect Predictor (VEP) to annotate unique SNVs using default parameters.
    # Consequence, CANONICAL, MANE, MAX_AF, MAX_AF_POPS, gnomADe_*, gnomADg_*
  input {
    File vcf_to_annot             # list of .parquet.tar.gz files
    Int cpu = 72             # default to 4 CPUs
    String project
  }

  # Derive chrom by removing known suffix from filename
  String chrom = basename(vcf_to_annot, ".vcf_bundle.tar.gz")

  command <<<
    # Install pigz inside the container
    apt-get update && apt-get install -y pigz


    # Download and extract Ensembl VEP cache (GRCh38, release 113)
    dx download "~{project}:SNV-Annotation/resources/vep_cache/homo_sapiens_vep_113_GRCh38.tar.gz"
    mkdir assembly_GRCh38
    tar --use-compress-program=pigz -xf homo_sapiens_vep_113_GRCh38.tar.gz -C assembly_GRCh38

    # Download and load Docker image for Ensembl VEP
    dx download "~{project}:SNV-Annotation/resources/dockers/ensembl-vep_113.3.tar"
    docker load -i ensembl-vep_113.3.tar

    # Extract the input VCF file to annotate
    tar --use-compress-program=pigz -xf ~{vcf_to_annot}

    # Pass DNAnexus environment variables to container
    env | grep '^DX_' > dx_env.list

    # Run VEP inside the Docker container
    docker run --user root --env-file dx_env.list --rm -v /home/dnanexus/:/home/dnanexus/ -v $(pwd):/data -w /data ensemblorg/ensembl-vep:release_113.3 bash -c "
          vep -i ~{chrom}.vcf.gz --format vcf \
          --cache --offline --fork ~{cpu} \
          --dir_cache=assembly_GRCh38 \
          --assembly GRCh38 \
          --force_overwrite --compress_output gzip --tab \
          --output_file ~{chrom}.tsv.gz \
          --stats_text --stats_html --stats_file ~{chrom}.txt \
          --canonical --mane --max_af --af_gnomade --af_gnomadg\
          --fields 'Uploaded_variation,Location,Allele,Gene,Feature,Consequence,CANONICAL,MANE,MAX_AF,MAX_AF_POPS,gnomADe_AF,gnomADe_AFR_AF,gnomADe_AMR_AF,gnomADe_ASJ_AF,gnomADe_EAS_AF,gnomADe_FIN_AF,gnomADe_MID_AF,gnomADe_NFE_AF,gnomADe_REMAINING_AF,gnomADe_SAS_AF,gnomADg_AF,gnomADg_AFR_AF,gnomADg_AMI_AF,gnomADg_AMR_AF,gnomADg_ASJ_AF,gnomADg_EAS_AF,gnomADg_FIN_AF,gnomADg_MID_AF,gnomADg_NFE_AF,gnomADg_REMAINING_AF,gnomADg_SAS_AF' \
          --verbose
    " 2>&1 | tee output.log
  >>>

  output {
    File stat  = "${chrom}.txt"
    File stat_html = "${chrom}.html"
    File tsv_vep  = "${chrom}.tsv.gz"
  }
  
  
  runtime {
    dx_instance_type: "mem1_ssd1_v2_x72"
  }
}





task RunVEPLoftee {
    # Runs Ensembl VEP with the LOFTEE plugin to predict Loss-Of-Function of SNVs.
  input {
    File vcf_to_annot             # list of .parquet.tar.gz files
    Int cpu = 72             # default to 4 CPUs
    String project
    
  }

  # Derive chrom by removing known suffix from filename
  String chrom = basename(vcf_to_annot, ".vcf_bundle.tar.gz")

  command <<<
    # Install pigz inside the container
    apt-get update && apt-get install -y pigz


    # Download and extract Ensembl VEP cache (GRCh38, release 113)
    dx download "~{project}:SNV-Annotation/resources/vep_cache/homo_sapiens_vep_113_GRCh38.tar.gz"
    mkdir assembly_GRCh38
    tar --use-compress-program=pigz -xf homo_sapiens_vep_113_GRCh38.tar.gz -C assembly_GRCh38

    # Download and load Docker image for Ensembl VEP
    dx download "~{project}:SNV-Annotation/resources/dockers/ensembl-vep_113.3.tar"
    docker load -i ensembl-vep_113.3.tar

    # Extract the input VCF file to annotate
    tar --use-compress-program=pigz -xf ~{vcf_to_annot}

    
    # Download LOFTEE resources
    dx download "~{project}:SNV-Annotation/resources/vep_cache/ressources_loftee.tar.gz"
    tar --use-compress-program=pigz -xf "ressources_loftee.tar.gz"
    


    # Pass DNAnexus environment variables to container
    env | grep '^DX_' > dx_env.list

    # Run VEP inside the Docker container
    docker run --user root --env-file dx_env.list --rm -v /home/dnanexus/:/home/dnanexus/ -v $(pwd):/data -w /data ensemblorg/ensembl-vep:release_113.3 bash -c "
            vep -i ~{chrom}.vcf.gz --format vcf \
              --cache --offline --fork ~{cpu} \
              --dir_cache=assembly_GRCh38 \
              --assembly GRCh38 \
              --force_overwrite --compress_output gzip --tab \
              --output_file ~{chrom}.tsv.gz \
              --stats_text --stats_html --stats_file ~{chrom}.txt \
              --plugin LoF,loftee_path:ressources_loftee/loftee-1.0.4_GRCh38/ \
              --dir_plugins ressources_loftee/loftee-1.0.4_GRCh38/ \
              --fields 'Uploaded_variation,Location,Allele,Gene,Feature,LoF,LoF_filter,LoF_flags,LoF_info' \
              --verbose
    " 2>&1 | tee output.log
  >>>

  output {
    File stat  = "${chrom}.txt"
    File stat_html = "${chrom}.html"
    File tsv_vep  = "${chrom}.tsv.gz"
  }
  
  runtime {
    dx_instance_type: "mem1_ssd1_v2_x72"
  }
}




task RunVEPAlphamissense {
    # Runs Ensembl VEP with the AlphaMissense plugin to predict missense effects of SNVs.
  input {
    File vcf_to_annot             # list of .parquet.tar.gz files
    Int cpu = 72             # default to 4 CPUs
    String project
  }

  # Derive chrom by removing known suffix from filename
  String chrom = basename(vcf_to_annot, ".vcf_bundle.tar.gz")

  command <<<
    # Install pigz inside the container
    apt-get update && apt-get install -y pigz


    # Download and extract Ensembl VEP cache (GRCh38, release 113)
    dx download "~{project}:SNV-Annotation/resources/vep_cache/homo_sapiens_vep_113_GRCh38.tar.gz"
    mkdir assembly_GRCh38
    tar --use-compress-program=pigz -xf homo_sapiens_vep_113_GRCh38.tar.gz -C assembly_GRCh38

    # Download and load Docker image for Ensembl VEP
    dx download "~{project}:SNV-Annotation/resources/dockers/ensembl-vep_113.3.tar"
    docker load -i ensembl-vep_113.3.tar

    # Extract the input VCF file to annotate
    tar --use-compress-program=pigz -xf ~{vcf_to_annot}


    # Download AlphaMissense resources
    dx download "~{project}:SNV-Annotation/resources/vep_cache/ressources_alphamissense.tar.gz"
    tar --use-compress-program=pigz -xf "ressources_alphamissense.tar.gz"


    # Pass DNAnexus environment variables to container
    env | grep '^DX_' > dx_env.list

    docker run --user root --env-file dx_env.list --rm -v /home/dnanexus/:/home/dnanexus/ -v $(pwd):/data -w /data ensemblorg/ensembl-vep:release_113.3 bash -c "
            vep -i ~{chrom}.vcf.gz --format vcf \
                --cache --offline --fork ~{cpu} \
                --dir_cache=assembly_GRCh38 \
                --assembly GRCh38 \
                --force_overwrite --compress_output gzip --tab \
                --output_file ~{chrom}.tsv.gz \
                --stats_text --stats_html --stats_file ~{chrom}.txt \
                --plugin AlphaMissense,file=ressources_alphamissense/AlphaMissense_hg38.tsv.gz \
                --fields 'Uploaded_variation,Location,Allele,Gene,Feature,am_class,am_pathogenicity' \
                --verbose
    " 2>&1 | tee output.log
  >>>

  output {
    File stat  = "${chrom}.txt"
    File stat_html = "${chrom}.html"
    File tsv_vep  = "${chrom}.tsv.gz"
  }
  
  runtime {
    dx_instance_type: "mem1_ssd1_v2_x72"
  }
}



task RunVEPSpliceAI {
    # Runs Ensembl VEP with the SpliceAI plugin to predict splice-altering effects of SNVs.
    # Need at least 100 GB of Storage due to cache
  input {
    File vcf_to_annot             # list of .parquet.tar.gz files
    Int cpu = 72             # default to 4 CPUs
    String project
  }

  # Derive chrom by removing known suffix from filename
  String chrom = basename(vcf_to_annot, ".vcf_bundle.tar.gz")

  command <<<
    # Install pigz inside the container
    apt-get update && apt-get install -y pigz


    # Download and extract Ensembl VEP cache (GRCh38, release 113)
    dx download "~{project}:SNV-Annotation/resources/vep_cache/homo_sapiens_vep_113_GRCh38.tar.gz"
    mkdir assembly_GRCh38
    tar --use-compress-program=pigz -xf homo_sapiens_vep_113_GRCh38.tar.gz -C assembly_GRCh38

    # Download and load Docker image for Ensembl VEP
    dx download "~{project}:SNV-Annotation/resources/dockers/ensembl-vep_113.3.tar"
    docker load -i ensembl-vep_113.3.tar

    # Extract the input VCF file to annotate
    tar --use-compress-program=pigz -xf ~{vcf_to_annot}


    # Download SpliceAI resources
    dx download "~{project}:SNV-Annotation/resources/vep_cache/ressources_spliceai.tar.gz"
    tar --use-compress-program=pigz -xf "ressources_spliceai.tar.gz"


    # Pass DNAnexus environment variables to container
    env | grep '^DX_' > dx_env.list

    docker run --user root --env-file dx_env.list --rm -v /home/dnanexus/:/home/dnanexus/ -v $(pwd):/data -w /data ensemblorg/ensembl-vep:release_113.3 bash -c "
            vep -i ~{chrom}.vcf.gz --format vcf \
                --cache --offline --fork ~{cpu} \
                --dir_cache=assembly_GRCh38 \
                --assembly GRCh38 \
                --force_overwrite --compress_output gzip --tab \
                --output_file ~{chrom}.tsv.gz \
                --stats_text --stats_html --stats_file ~{chrom}.txt \
                --plugin SpliceAI,snv=ressources_spliceai/spliceai_scores.raw.snv.hg38.vcf.gz,indel=ressources_spliceai/spliceai_scores.raw.indel.hg38.vcf.gz \
                --fields 'Uploaded_variation,Location,Allele,Gene,Feature,SpliceAI_pred' \
                --verbose
    " 2>&1 | tee output.log
  >>>

  output {
    File stat  = "${chrom}.txt"
    File stat_html = "${chrom}.html"
    File tsv_vep  = "${chrom}.tsv.gz"
  }
  
  runtime {
    dx_instance_type: "mem1_ssd1_v2_x72"
  }
}



task ConvertVEPOutParquet {
  # This rule converts VEP annotation output from compressed TSV format to Parquet, optimizing storage and retrieval.
  input {
    Array[File] list_tsv             # list tsv
    String plugin
    Int cpu = 96             # default to 96 CPUs
    Int mem_per_cpu = 4     # default to 4 GB per CPU
  }


  command <<<
    set -euo pipefail

    # Move input TSVs into the directory
    mkdir -p input_vcf_dir
    for file in ~{sep=' ' list_tsv}; do
      ln -s "$(realpath "$file")" input_vcf_dir/
    done

    # Run the Spark-based TSV-to-Parquet conversion script
    driver_memory=$(awk "BEGIN {printf \"%d\", ~{cpu} * ~{mem_per_cpu}}")
    /opt/spark/bin/spark-submit --driver-memory "$driver_memory"g /usr/bin/convert_vep_output_parquet.py "input_vcf_dir" "~{plugin}.parquet" ~{cpu} ~{mem_per_cpu} ~{plugin} 2>&1 | tee output.log

    # Compress the Parquet output directory into a single tar.gz archive for portability
    tar --use-compress-program=pigz -cf "~{plugin}.parquet.tar.gz" "~{plugin}.parquet"

  >>>

  output {
    File plugin_parquet_compressed = "~{plugin}.parquet.tar.gz"
  }

  runtime {
    docker: "flobenhsj/spark-tsv-to-parquet_v1.0:latest"
    dx_instance_type: "mem2_ssd2_v2_x96"
  }
  

}




task LossLessAnnotation {
    # This rule annotates all SNVs by merging unannotated SNVs with VEP default annotations and plugin-based annotations of unique SNVs.
  input {
    Array[File] plugins_parquet             # list tsv
    File vep_default_parquet
    File parquet_including_all_snv
    Int cpu = 96             # default to 96 CPUs
    Int mem_per_cpu = 7     # default to 4 GB per CPU
  }

  # Extract base names without the ".tar.gz" suffix for referencing directories after unpacking
  String file_vepdefault_name = basename(vep_default_parquet, ".tar.gz")
  String file_allsnv_name = basename(parquet_including_all_snv, ".tar.gz")


  command <<<
    set -euo pipefail

    # Unpack each archive into the uncompressed_parquets/ directory
    mkdir -p plugins_parquets
    for archive in ~{sep=' ' plugins_parquet}; do
      tar --use-compress-program=pigz -xf "$archive" -C plugins_parquets
    done

    # Generate a comma-separated list of all .parquet files
    plugin_files=$(find plugins_parquets -maxdepth 1 -name "*.parquet" | paste -sd, -)

    echo "Merging the following parquet files:"
    echo "$plugin_files"


    # Unpack the VEP default annotation Parquet archive
    tar --use-compress-program=pigz -xf ~{vep_default_parquet}

    # Unpack the Parquet archive containing all SNVs (raw input)
    tar --use-compress-program=pigz -xf ~{parquet_including_all_snv}

    # Run the Spark-based lossless annotation merge script
    driver_memory=$(awk "BEGIN {printf \"%d\", ~{cpu} * ~{mem_per_cpu}}")
    /opt/spark/bin/spark-submit --driver-memory "$driver_memory"g /usr/bin/lossless_annotation_v2.py ~{file_allsnv_name} ~{file_vepdefault_name} "$plugin_files" "snvDB_lossless.parquet" ~{cpu} ~{mem_per_cpu}  2>&1 | tee output.log

    # Compress the resulting lossless annotation parquet output directory
    tar --use-compress-program=pigz -cf "snvDB_lossless.parquet.tar.gz" "snvDB_lossless.parquet"

  >>>

  output {
    File lossless_parquet_compressed = "snvDB_lossless.parquet.tar.gz"
    File log_output = "output.log"
  }

  runtime {
    docker: "flobenhsj/spark-tsv-to-parquet_v1.0:latest"
    dx_instance_type: "mem3_ssd1_v2_x96"
  }
}




task RefinedAnnotation {
    # This rule produces a filtered parquet file commonly used for downstream analysis.
  input {
    File lossless_parquet
    Int cpu = 96             # default to 96 CPUs
    Int mem_per_cpu = 7     # default to 4 GB per CPU
  }

  String file_name = basename(lossless_parquet, ".tar.gz")

  command <<<
    set -euo pipefail

    # Unpack the lossless parquet archive into current directory
    tar --use-compress-program=pigz -xf ~{lossless_parquet}

    # Run Spark job to filter and summarize annotations
    driver_memory=$(awk "BEGIN {printf \"%d\", ~{cpu} * ~{mem_per_cpu}}")
    /opt/spark/bin/spark-submit --driver-memory "$driver_memory"g /usr/bin/refine_annotation.py ~{file_name} "snvDB_refined_summary.txt" "snvDB_refined.parquet" ~{cpu} ~{mem_per_cpu}

    # Compress the resulting refined parquet directory into a tar.gz archive
    tar --use-compress-program=pigz -cf "snvDB_refined.parquet.tar.gz" "snvDB_refined.parquet"

  >>>

  output {
    File summary_report = "snvDB_refined_summary.txt"
    File refined_parquet = "snvDB_refined.parquet.tar.gz"
  }

  runtime {
    docker: "flobenhsj/spark-tsv-to-parquet_v1.0:latest"
    dx_instance_type: "mem3_ssd1_v2_x96"
  }
  

}




task ProduceSummaryPDF {
    # This rule produces a filtered parquet file commonly used for downstream analysis.
  input {
    File snvs_annotated_parquet
    Int cpu = 96             # default to 96 CPUs
    Int mem_per_cpu = 4     # default to 4 GB per CPU
  }

  String file = basename(snvs_annotated_parquet, ".parquet.tar.gz")

  command <<<
    set -euo pipefail

    # Unpack the lossless parquet archive into current directory
    tar --use-compress-program=pigz -xf ~{snvs_annotated_parquet}

    python generate_pdf.py "~{file}.parquet" ~{cpu} ~{mem_per_cpu}

  >>>

  output {
    File pdf_file = "~{file}_dictionary.pdf"
  }
  
  runtime {
    docker: "flobenhsj/duckdb_python_v1.0:latest"
    dx_instance_type: "mem2_ssd2_v2_x96"
  }
  
}
