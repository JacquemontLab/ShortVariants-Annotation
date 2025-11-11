%%writefile ShortVariantsannotation.wdl
version 1.0

workflow process_gvcf_by_batch {

  input {
    File Unannotated_ShortVariants_parquet_compressed                   # Unannotated_ShortVariants obtained using HAIL
    File homo_sapiens_vep_113_GRCh38_tar_gz                    # homo_sapiens_vep_113_GRCh38_tar_gz
    File ressources_spliceai_tar_gz                            # SpliceAI resources for VEP
    File ressources_alphamissense_tar_gz                       # AlphaMissense resources for VEP
    File ressources_loftee_tar_gz                              # LOFTEE resources for VEP
  }

  call FindUniqShortVariantsVCF {
    input:
      parquet_including_all_ShortVariants = Unannotated_ShortVariants_parquet_compressed
  }

  # VEP Default Plugin
  scatter (vcf_to_annot in FindUniqShortVariantsVCF.vcf_tarballs) {
    call RunVEPDefault {
      input:
        vcf_to_annot = vcf_to_annot,
        homo_sapiens_vep_113_GRCh38_tar_gz = homo_sapiens_vep_113_GRCh38_tar_gz
    }
  }

  call ConvertVEPOutParquet as ConvertVEPOutDefault {
    input:
      list_tsv = RunVEPDefault.tsv_vep,
      plugin = "default"
  }

  # VEP SpliceAI Plugin
  scatter (vcf_to_annot in FindUniqShortVariantsVCF.vcf_tarballs) {
    call RunVEPSpliceAI {
      input:
        vcf_to_annot = vcf_to_annot,
        homo_sapiens_vep_113_GRCh38_tar_gz = homo_sapiens_vep_113_GRCh38_tar_gz,
        ressources_spliceai_tar_gz = ressources_spliceai_tar_gz
    }
  }

  call ConvertVEPOutParquet as ConvertVEPOutSpliceAI {
    input:
      list_tsv = RunVEPSpliceAI.tsv_vep,
      plugin = "spliceai"
  }

  # VEP Alphamissense Plugin
  scatter (vcf_to_annot in FindUniqShortVariantsVCF.vcf_tarballs) {
    call RunVEPAlphamissense {
      input:
        vcf_to_annot = vcf_to_annot,
        homo_sapiens_vep_113_GRCh38_tar_gz = homo_sapiens_vep_113_GRCh38_tar_gz,
        ressources_alphamissense_tar_gz = ressources_alphamissense_tar_gz
    }
  }

  call ConvertVEPOutParquet as ConvertVEPOutAlphamissense {
    input:
      list_tsv = RunVEPAlphamissense.tsv_vep,
      plugin = "alphamissense"
  }

  # VEP LOFTEE Plugin
  scatter (vcf_to_annot in FindUniqShortVariantsVCF.vcf_tarballs) {
    call RunVEPLoftee {
      input:
        vcf_to_annot = vcf_to_annot,
        homo_sapiens_vep_113_GRCh38_tar_gz = homo_sapiens_vep_113_GRCh38_tar_gz,
        ressources_loftee_tar_gz = ressources_loftee_tar_gz
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
      parquet_including_all_ShortVariants = Unannotated_ShortVariants_parquet_compressed
  }

  call RefinedAnnotation {
    input:
      lossless_parquet = LossLessAnnotation.lossless_parquet_compressed
  }

  output {
    Array[File] vcf_tarballs = FindUniqShortVariantsVCF.vcf_tarballs
    Array[File] vep_output_txt = RunVEPDefault.stat
    Array[File] vep_output_html = RunVEPDefault.stat_html
    Array[File] vep_output_tsv = RunVEPDefault.tsv_vep
    File refined_parquet = RefinedAnnotation.refined_parquet
    File summary_report = RefinedAnnotation.summary_report
    File lossless_parquet = LossLessAnnotation.lossless_parquet_compressed
    File lossless_annotation_log = LossLessAnnotation.log_output
  }
}



task FindUniqShortVariantsVCF {
    # This rule extracts unique short variants (SNVs and Indels) from a large Parquet file and generates chromosome-specific VCF files
  input {
    File parquet_including_all_ShortVariants             # list of .parquet.tar.gz files
    Int cpu = 96
    Int mem_per_cpu = 4
    Int boot_disk_gb = 5
    Int disk_gb = 1500
  }

  # Derive file name by removing known suffix from filename
  String parquet_name = basename(parquet_including_all_ShortVariants, ".tar.gz")

  command <<<
    set -euo pipefail

    # Decompress the input archive (~{parquet_including_all_ShortVariants}) using pigz
    tar --use-compress-program=pigz -xf ~{parquet_including_all_ShortVariants}
    
    # Run PySpark script to generate per-chromosome VCFs of unique short variants (SNVs and Indels) from the input Parquet
    mkdir -p "data/processed/vcf_uniq_ShortVariants/"
    driver_memory=$(awk "BEGIN {printf \"%d\", ~{cpu} * ~{mem_per_cpu}}")
    
    /opt/spark/bin/spark-submit \
          --conf spark.driver.memory="${driver_memory}G" \
          --conf spark.driver.cores="~{cpu}" \
          /usr/bin/generate_parquet_uniq_ShortVariants.py ~{parquet_name} "data/processed/vcf_uniq_ShortVariants/" ~{cpu} ~{mem_per_cpu} 2>&1 | tee output.log

    
    # Post-process VCF files: sort, compress, index
    for file in data/processed/vcf_uniq_ShortVariants/chr*.vcf; do

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
    Array[File] vcf_tarballs = glob("data/processed/vcf_uniq_ShortVariants/*.vcf_bundle.tar.gz")
  }

  runtime {
    docker: 'gcr.io/softwarepgc/spark-tsv-to-parquet_v1.0:latest'
    memory: (cpu * mem_per_cpu) + " GB"
    disks: "local-disk " + disk_gb + " HDD"
    bootDiskSizeGb: boot_disk_gb
    cpu: cpu
  }
}


task RunVEPDefault {
    # This rule runs the Ensembl Variant Effect Predictor (VEP) to annotate unique short variants (SNVs and Indels) using default parameters.
    # Consequence, CANONICAL, MANE, MAX_AF, MAX_AF_POPS, gnomADe_*, gnomADg_*
  input {
    File vcf_to_annot             # list of .parquet.tar.gz files
    File homo_sapiens_vep_113_GRCh38_tar_gz # reference genome for VEP
    Int cpu = 72
    Int mem_per_cpu = 4
    Int boot_disk_gb = 5
    Int disk_gb = 500
  }

  # Derive chrom by removing known suffix from filename
  String chrom = basename(vcf_to_annot, ".vcf_bundle.tar.gz")

  command <<<
    # Download and extract Ensembl VEP cache (GRCh38, release 113)
    mkdir assembly_GRCh38
    tar --use-compress-program=pigz -xf ~{homo_sapiens_vep_113_GRCh38_tar_gz} -C assembly_GRCh38

    # Extract the input VCF file to annotate
    tar --use-compress-program=pigz -xf ~{vcf_to_annot}

    # Run VEP inside the Docker container
    bash -c "
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
    docker: 'gcr.io/softwarepgc/ensembl-vep_113.3:latest'
    memory: (cpu * mem_per_cpu) + " GB"
    disks: "local-disk " + disk_gb + " HDD"
    bootDiskSizeGb: boot_disk_gb
    cpu: cpu
  } 
}

task RunVEPLoftee {
    # Runs Ensembl VEP with the LOFTEE plugin to predict Loss-Of-Function of short variants (SNVs and Indels).
  input {
    File vcf_to_annot             # list of .parquet.tar.gz files
    File homo_sapiens_vep_113_GRCh38_tar_gz # reference genome for VEP
    File ressources_loftee_tar_gz # LOFTEE resources for VEP
    Int cpu = 72
    Int mem_per_cpu = 4
    Int boot_disk_gb = 5
    Int disk_gb = 500
  }

  # Derive chrom by removing known suffix from filename
  String chrom = basename(vcf_to_annot, ".vcf_bundle.tar.gz")

  command <<<
    # Download and extract Ensembl VEP cache (GRCh38, release 113)
    mkdir assembly_GRCh38
    tar --use-compress-program=pigz -xf ~{homo_sapiens_vep_113_GRCh38_tar_gz} -C assembly_GRCh38

    # Extract the input VCF file to annotate
    tar --use-compress-program=pigz -xf ~{vcf_to_annot}

    # Download LOFTEE resources
    tar --use-compress-program=pigz -xf ~{ressources_loftee_tar_gz}

    # Run VEP inside the Docker container
    bash -c "
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
    docker: 'gcr.io/softwarepgc/ensembl-vep_113.3:latest'
    memory: (cpu * mem_per_cpu) + " GB"
    disks: "local-disk " + disk_gb + " HDD"
    bootDiskSizeGb: boot_disk_gb
    cpu: cpu
  } 
}

task RunVEPAlphamissense {
    # Runs Ensembl VEP with the AlphaMissense plugin to predict missense effects of short variants (SNVs and Indels).
  input {
    File vcf_to_annot             # list of .parquet.tar.gz files
    File homo_sapiens_vep_113_GRCh38_tar_gz # reference genome for VEP
    File ressources_alphamissense_tar_gz # AlphaMissense resources for VEP
    Int cpu = 72
    Int mem_per_cpu = 4
    Int boot_disk_gb = 5
    Int disk_gb = 500
  }

  # Derive chrom by removing known suffix from filename
  String chrom = basename(vcf_to_annot, ".vcf_bundle.tar.gz")

  command <<<
    # Download and extract Ensembl VEP cache (GRCh38, release 113)
    mkdir assembly_GRCh38
    tar --use-compress-program=pigz -xf ~{homo_sapiens_vep_113_GRCh38_tar_gz} -C assembly_GRCh38

    # Extract the input VCF file to annotate
    tar --use-compress-program=pigz -xf ~{vcf_to_annot}

    # Download AlphaMissense resources
    tar --use-compress-program=pigz -xf ~{ressources_alphamissense_tar_gz}

    bash -c "
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
    docker: 'gcr.io/softwarepgc/ensembl-vep_113.3:latest'
    memory: (cpu * mem_per_cpu) + " GB"
    disks: "local-disk " + disk_gb + " HDD"
    bootDiskSizeGb: boot_disk_gb
    cpu: cpu
  } 
}

task RunVEPSpliceAI {
    # Runs Ensembl VEP with the SpliceAI plugin to predict splice-altering effects of short variants (SNVs and Indels).
    # Need at least 100 GB of Storage due to cache
  input {
    File vcf_to_annot             # list of .parquet.tar.gz files
    File homo_sapiens_vep_113_GRCh38_tar_gz # reference genome for VEP
    File ressources_spliceai_tar_gz # SpliceAI resources for VEP
    Int cpu = 72
    Int mem_per_cpu = 4
    Int boot_disk_gb = 5
    Int disk_gb = 500
  }

  # Derive chrom by removing known suffix from filename
  String chrom = basename(vcf_to_annot, ".vcf_bundle.tar.gz")

  command <<<
    # Download and extract Ensembl VEP cache (GRCh38, release 113)
    mkdir assembly_GRCh38
    tar --use-compress-program=pigz -xf ~{homo_sapiens_vep_113_GRCh38_tar_gz} -C assembly_GRCh38

    # Extract the input VCF file to annotate
    tar --use-compress-program=pigz -xf ~{vcf_to_annot}

    # Download SpliceAI resources
    tar --use-compress-program=pigz -xf ~{ressources_spliceai_tar_gz}

    bash -c "
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
    docker: 'gcr.io/softwarepgc/ensembl-vep_113.3:latest'
    memory: (cpu * mem_per_cpu) + " GB"
    disks: "local-disk " + disk_gb + " HDD"
    bootDiskSizeGb: boot_disk_gb
    cpu: cpu
  } 
}

task ConvertVEPOutParquet {
  # This rule converts VEP annotation output from compressed TSV format to Parquet, optimizing storage and retrieval.
  input {
    Array[File] list_tsv             # list tsv
    String plugin
    Int cpu = 96
    Int mem_per_cpu = 4
    Int boot_disk_gb = 5
    Int disk_gb = 500
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
    
    /opt/spark/bin/spark-submit \
          --conf spark.driver.memory="${driver_memory}G" \
          --conf spark.driver.cores="~{cpu}" \
          --driver-memory "$driver_memory"g /usr/bin/convert_vep_output_parquet.py "input_vcf_dir" "~{plugin}.parquet" ~{cpu} ~{mem_per_cpu} ~{plugin} 2>&1 | tee output.log

    # Compress the Parquet output directory into a single tar.gz archive for portability
    tar --use-compress-program=pigz -cf "~{plugin}.parquet.tar.gz" "~{plugin}.parquet"

  >>>

  output {
    File plugin_parquet_compressed = "~{plugin}.parquet.tar.gz"
  }

  runtime {
    docker: 'gcr.io/softwarepgc/spark-tsv-to-parquet_v1.0:latest'
    memory: (cpu * mem_per_cpu) + " GB"
    disks: "local-disk " + disk_gb + " HDD"
    bootDiskSizeGb: boot_disk_gb
    cpu: cpu
  } 

}

task LossLessAnnotation {
    # This rule annotates all short variants (SNVs and Indels) by merging unannotated short variants with VEP default annotations and plugin-based annotations of unique short variants.
  input {
    Array[File] plugins_parquet             # list tsv
    File vep_default_parquet
    File parquet_including_all_ShortVariants
    Int cpu = 96
    Int mem_per_cpu = 6
    Int boot_disk_gb = 5
    Int disk_gb = 3000
  }

  # Extract base names without the ".tar.gz" suffix for referencing directories after unpacking
  String file_vepdefault_name = basename(vep_default_parquet, ".tar.gz")
  String file_allShortVariants_name = basename(parquet_including_all_ShortVariants, ".tar.gz")


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

    # Unpack the Parquet archive containing all short variants (SNVs and Indels) (raw input)
    tar --use-compress-program=pigz -xf ~{parquet_including_all_ShortVariants}

    # Run the Spark-based lossless annotation merge script
    driver_memory=$(awk "BEGIN {printf \"%d\", ~{cpu} * ~{mem_per_cpu}}")
    
    mkdir -p /home/jupyter/tmp_spark
    export SPARK_LOCAL_DIRS=/home/jupyter/tmp_spark
    
    /opt/spark/bin/spark-submit \
          --conf spark.driver.memory="${driver_memory}G" \
          --conf spark.driver.cores="~{cpu}" \
          --driver-memory "$driver_memory"g /usr/bin/lossless_annotation.py ~{file_allShortVariants_name} ~{file_vepdefault_name} "$plugin_files" "ShortVariantsDB_lossless.parquet" ~{cpu} ~{mem_per_cpu}  2>&1 | tee output.log

    rm -rf /home/jupyter/tmp_spark
    
    # Compress the resulting lossless annotation parquet output directory
    tar --use-compress-program=pigz -cf "ShortVariantsDB_lossless.parquet.tar.gz" "ShortVariantsDB_lossless.parquet"

  >>>

  output {
    File lossless_parquet_compressed = "ShortVariantsDB_lossless.parquet.tar.gz"
    File log_output = "output.log"
  }

  runtime {
    docker: 'gcr.io/softwarepgc/spark-tsv-to-parquet_v1.0:latest'
    memory: "624 GB"
    disks: "local-disk " + disk_gb + " HDD"
    bootDiskSizeGb: boot_disk_gb
    cpu: cpu
  } 
}

task RefinedAnnotation {
    # This rule produces a filtered parquet file commonly used for downstream analysis.
  input {
    File lossless_parquet
    Int cpu = 96
    Int mem_per_cpu = 6
    Int boot_disk_gb = 5
    Int disk_gb = 3000
  }

  String file_name = basename(lossless_parquet, ".tar.gz")

  command <<<
    set -euo pipefail

    # Unpack the lossless parquet archive into current directory
    tar --use-compress-program=pigz -xf ~{lossless_parquet}

    # Run Spark job to filter and summarize annotations
    driver_memory=$(awk "BEGIN {printf \"%d\", ~{cpu} * ~{mem_per_cpu}}")

    mkdir -p /home/jupyter/tmp_spark
    export SPARK_LOCAL_DIRS=/home/jupyter/tmp_spark
    
    /opt/spark/bin/spark-submit \
          --conf spark.driver.memory="${driver_memory}G" \
          --conf spark.driver.cores="~{cpu}" \
          --driver-memory "$driver_memory"g /usr/bin/refine_annotation.py ~{file_name} "ShortVariantsDB_refined_summary.txt" "ShortVariantsDB_refined.parquet" ~{cpu} ~{mem_per_cpu}

    rm -rf /home/jupyter/tmp_spark
    
    # Compress the resulting refined parquet directory into a tar.gz archive
    tar --use-compress-program=pigz -cf "ShortVariantsDB_refined.parquet.tar.gz" "ShortVariantsDB_refined.parquet"

  >>>

  output {
    File summary_report = "ShortVariantsDB_refined_summary.txt"
    File refined_parquet = "ShortVariantsDB_refined.parquet.tar.gz"
  }

  runtime {
    docker: 'gcr.io/softwarepgc/spark-tsv-to-parquet_v1.0:latest'
    memory: "624 GB"
    disks: "local-disk " + disk_gb + " HDD"
    bootDiskSizeGb: boot_disk_gb
    cpu: cpu
  }

}

task ProduceSummaryPDF {
    # This rule produces a filtered parquet file commonly used for downstream analysis.
  input {
    File ShortVariants_annotated_parquet
    Int cpu = 96
    Int mem_per_cpu = 6
    Int boot_disk_gb = 5
    Int disk_gb = 500
  }

  String file = basename(ShortVariants_annotated_parquet, ".parquet.tar.gz")

  command <<<
    set -euo pipefail

    # Unpack the lossless parquet archive into current directory
    tar --use-compress-program=pigz -xf ~{ShortVariants_annotated_parquet}

    python /usr/bin/pdf_dictionnary.py "~{file}.parquet" ~{cpu} ~{mem_per_cpu}
  >>>

  output {
    File pdf_file = "~{file}_dictionary.pdf"
  }
  
  runtime {
    docker: 'gcr.io/softwarepgc/duckdb_python_v1.0:latest'
    memory: "624 GB"
    disks: "local-disk " + disk_gb + " HDD"
    bootDiskSizeGb: boot_disk_gb
    cpu: cpu
  }
}
