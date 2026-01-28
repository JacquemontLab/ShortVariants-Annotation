#!/usr/bin/env nextflow

nextflow.enable.dsl=2



// This rule splits a large sample list into multiple smaller files (batches) 
// to facilitate parallel processing. Each batch contains approximately equal 
// numbers of lines, ensuring balanced workload distribution.
process SplitSampleList {

    input:
    path sample_list
    val num_batches
    val count_start

    output:
    path "batch_*", emit: batch_files

    script:
    """

    # Count total lines
    total_lines=\$(wc -l < "${sample_list}")

    # Determine number of lines per batch
    lines_per_batch=\$(( (total_lines + ${num_batches} - 1) / ${num_batches} ))

    # Split the file into num_batches files
    split -d -a 4 -l \$lines_per_batch "${sample_list}" "batch_"

    # Rename files to start numbering at count_start
    files=( \$(ls -v batch_*) )
    total_files=\${#files[@]}

    for (( i=total_files-1; i>=0; i-- )); do
        new_index=\$(( ${count_start} + i ))
        mv "\${files[i]}" "batch_\$(printf '%04d' \$new_index)"
    done
    """
}

// This rule processes per-sample gVCF files for the UKBB dataset for a given batch.
// It extracts SNPs, INDELs, and Non Homozygous reference sites from variant calls.
// The processed data is saved as a compressed TSV file for each sample.
process ProduceTSVPerSample {
    label "genomics_tools"
    tag "batch_${batch}"

    input:
    val batch
    path list_sample_to_process
    path file_gvcf_path
    path fasta_ref

    output:
    path "batch_${batch}_gvcf_produced", emit: batch_done

    script:
    """
    echo "Using ${task.cpus} CPUs"
    
    export OUTDIR="batch_${batch}_gvcf_produced"

    mkdir -p "\$OUTDIR"

    process_sample() {
        sample=\$1
        input_gvcf=\$(zgrep "\${sample}" "${file_gvcf_path}" | cut -f2)

        timedev -v bash -c "
            extraction_snps_indels_psychencode.sh \
                \\"\${sample}\\" \
                \\"\${input_gvcf}\\" \
                \\"\${OUTDIR}/\${sample}.tsv.gz\\" \
                \\"${fasta_ref}\\" \
                \\"${task.cpus}\\"
        "
    }

    export -f process_sample

    cat ${list_sample_to_process} | parallel -j ${task.cpus} process_sample {}

    """
}

// This rule merges multiple TSV files containing gVCF data into a single Parquet file for a given batch.
// Note : 1sec per samples, PySpark is able to multithread but it might not necessarily changed the process time
process MergeTSVParquetByBatch {
    label "pyspark"
    tag "batch: ${batch}"

    input:
    val batch
    path batch_done

    output:
    path "Unannotated_ShortVariants_batch_${batch}.parquet"

    script:
    // Compute memory in GB if task.memory exists; else leave null
    def memGb = task.memory ? (task.memory.toGiga() * 0.90).intValue() : null

    """
    # If memGb > 0 (HPC), use it; else detect 90% of VM RAM
    if [ ${memGb} -gt 0 ]; then
        MEM_GB=${memGb}
    else
        MEM_GB=\$(free -k | awk '/^Mem:/ {print int(\$2*0.90/1024/1024)}')
    fi

    echo "Using memory limit: \${MEM_GB} GB"

    MEM_PER_CPU_GB=\$(( MEM_GB / ${task.cpus} ))

    echo "Using ${task.cpus} CPUs"
    echo "Memory per CPU: \${MEM_PER_CPU_GB} GB"

    timedev -v generate_parquet_all_ShortVariants.py \
        ${batch_done}/ \
        Unannotated_ShortVariants_batch_${batch}.parquet \
        ${task.cpus} \${MEM_PER_CPU_GB}
    """
}


// This rule merges multiple batch-level Parquet files into a single consolidated Parquet file.
// Note : 1sec per samples, PySpark is able to multithread but it might not necessarily changed the process time
process MergeBatches {
    label "pyspark"

    input:
    path parquet_files

    output:
    path "Unannotated_ShortVariants.parquet"

    script:
    // Join batch paths as comma-separated string
    def batches_files = parquet_files.collect { it }.join(',')

    // Compute memory in GB if task.memory exists; else leave null
    def memGb = task.memory ? (task.memory.toGiga() * 0.90).intValue() : null

    """
    # If memGb > 0 (HPC), use it; else detect 90% of VM RAM
    if [ ${memGb} -gt 0 ]; then
        MEM_GB=${memGb}
    else
        MEM_GB=\$(free -k | awk '/^Mem:/ {print int(\$2*0.90/1024/1024)}')
    fi

    echo "Using memory limit: \${MEM_GB} GB"

    MEM_PER_CPU_GB=\$(( MEM_GB / ${task.cpus} ))

    echo "Using ${task.cpus} CPUs"
    echo "Memory per CPU: \${MEM_PER_CPU_GB} GB"
    
    echo "Merging batches:"
    echo "${batches_files}"

    timedev -v merge_parquets.py \
        ${batches_files} \
        Unannotated_ShortVariants.parquet \
        ${task.cpus} \${MEM_PER_CPU_GB}
    """
}

// This rule extracts unique short variants (SNVs and Indels) from a large Parquet file and generates chromosome-specific VCF files
process FindUniqShortVariantsVCF {
    label "pyspark"

    input:
    path parquet_file  // the consolidated Parquet file

    output:
    path "vcf_uniq_ShortVariants/*.vcf.gz", emit: chrom_vcfs

    script:
    // Compute memory in GB if task.memory exists; else leave null
    def memGb = task.memory ? (task.memory.toGiga() * 0.90).intValue() : null

    """
    # If memGb > 0 (HPC), use it; else detect 90% of VM RAM
    if [ ${memGb} -gt 0 ]; then
        MEM_GB=${memGb}
    else
        MEM_GB=\$(free -k | awk '/^Mem:/ {print int(\$2*0.90/1024/1024)}')
    fi

    echo "Using memory limit: \${MEM_GB} GB"

    MEM_PER_CPU_GB=\$(( MEM_GB / ${task.cpus} ))

    echo "Using ${task.cpus} CPUs"
    echo "Memory per CPU: \${MEM_PER_CPU_GB} GB"

    mkdir -p vcf_uniq_ShortVariants/

    # Extract unique ShortVariants into chromosome-specific VCFs
    timedev -v generate_parquet_uniq_ShortVariants.py \
        ${parquet_file} \
        vcf_uniq_ShortVariants/ \
        ${task.cpus} \${MEM_PER_CPU_GB}

    # Sort, compress, index, and clean VCFs
    for file in vcf_uniq_ShortVariants/chr*.vcf; do
        vcf-sort "\$file" > "\${file%.vcf}_sorted.vcf"
        mv "\${file%.vcf}_sorted.vcf" "\$file"
        bgzip -c "\$file" > "\$file.gz"
        tabix -p vcf "\$file.gz"
        rm "\$file"
    done
    """
}


// This rule runs the Ensembl Variant Effect Predictor (VEP) to annotate unique short variants (SNVs and Indels) using default parameters.
// Consequence, CANONICAL, MANE, MAX_AF, MAX_AF_POPS, gnomADe_*, gnomADg_*
process RunVEPDefault {
    label "ensembl_vep_113"
    tag "VEP annotation: ${chrom}"

    input:
    val chrom
    path vcf_file

    output:
    path "default/${chrom}.tsv.gz", emit: tsv_vep
    path "default/${chrom}.txt", emit: stat
    path "default/${chrom}.html", emit: stat_html


    script:
    """
    mkdir -p default/

    echo "Annotating chromosome ${chrom} with VEP using ${task.cpus} CPUs"

    timedev -v apptainer exec --bind ${workflow.projectDir}:${workflow.projectDir} \
        /home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/SOFTWARE/Dockers/ensembl_113_latest.sif \
        vep -i ${vcf_file} --format vcf \
            --cache --offline --fork ${task.cpus} \
            --dir_cache=/home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/SOFTWARE/VEP/cache/ \
            --assembly GRCh38 \
            --force_overwrite --compress_output gzip --tab \
            --output_file default/${chrom}.tsv.gz \
            --stats_text --stats_html --stats_file default/${chrom}.txt \
            --canonical --mane --max_af --af_gnomade --af_gnomadg \
            --fields "Uploaded_variation,Location,Allele,Gene,Feature,Consequence,CANONICAL,MANE,MAX_AF,MAX_AF_POPS,gnomADe_AF,gnomADe_AFR_AF,gnomADe_AMR_AF,gnomADe_ASJ_AF,gnomADe_EAS_AF,gnomADe_FIN_AF,gnomADe_MID_AF,gnomADe_NFE_AF,gnomADe_REMAINING_AF,gnomADe_SAS_AF,gnomADg_AF,gnomADg_AFR_AF,gnomADg_AMI_AF,gnomADg_AMR_AF,gnomADg_ASJ_AF,gnomADg_EAS_AF,gnomADg_FIN_AF,gnomADg_MID_AF,gnomADg_NFE_AF,gnomADg_REMAINING_AF,gnomADg_SAS_AF" \
            --verbose
    """
}


// Runs Ensembl VEP with the LOFTEE plugin to predict Loss-Of-Function of short variants (SNVs and Indels).
process RunVEPLoftee {
    label "ensembl_vep_113"
    tag "VEP Loftee: ${chrom}"

    input:
    val chrom
    path vcf_file

    output:
    path "loftee/${chrom}.tsv.gz", emit: tsv_vep
    path "loftee/${chrom}.txt", emit: stat
    path "loftee/${chrom}.html", emit: stat_html


    script:
    """
    mkdir -p loftee/

    echo "Annotating chromosome ${chrom} with VEP using ${task.cpus} CPUs"

    timedev -v apptainer exec --bind ${workflow.projectDir}:${workflow.projectDir} \
        /home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/SOFTWARE/Dockers/ensembl_113_latest.sif \
        vep -i ${vcf_file} --format vcf \
            --cache --offline --fork ${task.cpus} \
            --dir_cache=/home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/SOFTWARE/VEP/cache/ \
            --assembly GRCh38 \
            --force_overwrite --compress_output gzip --tab \
            --output_file loftee/${chrom}.tsv.gz \
            --stats_text --stats_html --stats_file loftee/${chrom}.txt \
            --plugin LoF,loftee_path:/home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/SOFTWARE/VEP/cache/ressources_loftee/loftee-1.0.4_GRCh38/ \
            --dir_plugins /home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/SOFTWARE/VEP/cache/ressources_loftee/loftee-1.0.4_GRCh38/ \
            --fields "Uploaded_variation,Location,Allele,Gene,Feature,LoF,LoF_filter,LoF_flags,LoF_info" \
            --verbose
    """
}


// Runs Ensembl VEP with the AlphaMissense plugin to predict missense effects of short variants (SNVs and Indels).
process RunVEPAlphamissense {
    label "ensembl_vep_113"
    tag "VEP Alphamissense: ${chrom}"

    input:
    val chrom
    path vcf_file

    output:
    path "alphamissense/${chrom}.tsv.gz", emit: tsv_vep
    path "alphamissense/${chrom}.txt", emit: stat
    path "alphamissense/${chrom}.html", emit: stat_html


    script:
    """
    mkdir -p alphamissense/

    echo "Annotating chromosome ${chrom} with VEP using ${task.cpus} CPUs"

    timedev -v apptainer exec --bind ${workflow.projectDir}:${workflow.projectDir} \
        /home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/SOFTWARE/Dockers/ensembl_113_latest.sif \
        vep -i ${vcf_file} --format vcf \
            --cache --offline --fork ${task.cpus} \
            --dir_cache=/home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/SOFTWARE/VEP/cache/ \
            --assembly GRCh38 \
            --force_overwrite --compress_output gzip --tab \
            --output_file alphamissense/${chrom}.tsv.gz \
            --stats_text --stats_html --stats_file alphamissense/${chrom}.txt \
            --plugin AlphaMissense,file=/home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/SOFTWARE/VEP/cache/ressources_alphamissense/AlphaMissense_hg38.tsv.gz \
            --fields "Uploaded_variation,Location,Allele,Gene,Feature,am_class,am_pathogenicity" \
            --verbose
    """
}


// Runs Ensembl VEP with the SpliceAI plugin to predict splice-altering effects of short variants (SNVs and Indels).
// Need at least 100 GB of Storage due to cache
process RunVEPSpliceAI {
    label "ensembl_vep_113"
    tag "VEP SpliceAI: ${chrom}"

    input:
    val chrom
    path vcf_file

    output:
    path "spliceai/${chrom}.tsv.gz", emit: tsv_vep
    path "spliceai/${chrom}.txt", emit: stat
    path "spliceai/${chrom}.html", emit: stat_html


    script:
    """
    mkdir -p spliceai/

    echo "Annotating chromosome ${chrom} with VEP using ${task.cpus} CPUs"

    timedev -v apptainer exec --bind ${workflow.projectDir}:${workflow.projectDir} \
        /home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/SOFTWARE/Dockers/ensembl_113_latest.sif \
        vep -i ${vcf_file} --format vcf \
            --cache --offline --fork ${task.cpus} \
            --dir_cache=/home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/SOFTWARE/VEP/cache/ \
            --assembly GRCh38 \
            --force_overwrite --compress_output gzip --tab \
            --output_file spliceai/${chrom}.tsv.gz \
            --stats_text --stats_html --stats_file spliceai/${chrom}.txt \
            --plugin SpliceAI,snv=/home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/SOFTWARE/VEP/cache/ressources_spliceai/spliceai_scores.raw.snv.hg38.vcf.gz,indel=/home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/SOFTWARE/VEP/cache/ressources_spliceai/spliceai_scores.raw.indel.hg38.vcf.gz \
            --fields "Uploaded_variation,Location,Allele,Gene,Feature,SpliceAI_pred" \
            --verbose
    """
}

// This rule converts VEP annotation output from compressed TSV format to Parquet, optimizing storage and retrieval.
include { ConvertVEPOutParquet as ConvertVEPDefaultParquet } from './vep_processes.nf'
include { ConvertVEPOutParquet as ConvertVEPLofteeParquet } from './vep_processes.nf'
include { ConvertVEPOutParquet as ConvertVEPAlphamissenseParquet } from './vep_processes.nf'
include { ConvertVEPOutParquet as ConvertVEPSpliceAIParquet } from './vep_processes.nf'



// This rule annotates all short variants (SNVs and Indels) by merging unannotated short variants with VEP default annotations and plugin-based annotations of unique short variants.
process UnfilteredAnnotation {
    label "pyspark"

    input:
    path all_plugin_parquets          // list of plugin files
    path vep_default_path      // default VEP parquet
    path parquet_including_all_ShortVariants  // unannotated ShortVariants parquet

    output:
    path "ShortVariantsDB_unfiltered.parquet", emit: ShortVariants_annotated_parquet

    script:
    // Compute memory in GB if task.memory exists; else leave null
    def memGb = task.memory ? (task.memory.toGiga() * 0.90).intValue() : null

    // Build comma-separated list of plugin parquet files
    def plugin_files = all_plugin_parquets.collect { it }.join(',')

    """
    # If memGb > 0 (HPC), use it; else detect 90% of VM RAM
    if [ ${memGb} -gt 0 ]; then
        MEM_GB=${memGb}
    else
        MEM_GB=\$(free -k | awk '/^Mem:/ {print int(\$2*0.90/1024/1024)}')
    fi

    MEM_PER_CPU_GB=\$(( MEM_GB / ${task.cpus} ))

    echo "Using memory limit: \${MEM_GB} GB"
    echo "Using ${task.cpus} CPUs"
    echo "Memory per CPU: \${MEM_PER_CPU_GB} GB"

    mkdir -p tmp_spark
    export SPARK_LOCAL_DIRS=tmp_spark

    timedev -v unfiltered_annotation.py \
        ${parquet_including_all_ShortVariants} \
        ${vep_default_path} \
        ${plugin_files} \
        ShortVariantsDB_unfiltered.parquet \
        ${task.cpus} \
        \${MEM_PER_CPU_GB}

    rm -rf tmp_spark
    """
}


// This rule produces a filtered parquet file commonly used for downstream analysis.
process CuratedAnnotation {
    label "pyspark"

    input:
    path ShortVariants_annotated_parquet

    output:
    path "ShortVariantsDB_curated_summary.txt", emit: summary_report
    path "ShortVariantsDB_curated.parquet", emit: curated_parquet


    script:
    // Compute memory in GB if task.memory exists; else leave null
    def memGb = task.memory ? (task.memory.toGiga() * 0.90).intValue() : null

    """
    # If memGb > 0 (HPC), use it; else detect 90% of VM RAM
    if [ ${memGb} -gt 0 ]; then
        MEM_GB=${memGb}
    else
        MEM_GB=\$(free -k | awk '/^Mem:/ {print int(\$2*0.90/1024/1024)}')
    fi

    MEM_PER_CPU_GB=\$(( MEM_GB / ${task.cpus} ))

    echo "Using memory limit: \${MEM_GB} GB"
    echo "Using ${task.cpus} CPUs"
    echo "Memory per CPU: \${MEM_PER_CPU_GB} GB"
    
    mkdir -p tmp_spark
    export SPARK_LOCAL_DIRS=tmp_spark

    timedev -v curated_annotation.py \
        ${ShortVariants_annotated_parquet} \
        ShortVariantsDB_curated_summary.txt \
        ShortVariantsDB_curated.parquet \
        ${task.cpus} \${MEM_PER_CPU_GB}

    rm -rf tmp_spark
    """
}




// This rule produces a filtered parquet file commonly used for downstream analysis.
include { ProduceSummaryPDF as ProduceSummaryPDF_Curated } from './vep_processes.nf'
include { ProduceSummaryPDF as ProduceSummaryPDF_Unfiltered } from './vep_processes.nf'



// Parameters
params.file_gvcf_path = "${projectDir}/tests/sample_to_gvcf.tsv.gz"
params.sample_list = "${projectDir}/tests/list_sample.txt"
params.fasta_ref = "/home/flben/links/projects/rrg-jacquese/LAB_WORKSPACE/RAW_DATA/Genetic/Reference_Data/reference_genome/GRCh38_full_analysis_set_plus_decoy_hla.fa.gz"
params.num_batches = 4
params.count_start = 1

workflow {

    // Split the sample list into batches
    split_ch = SplitSampleList(
        params.sample_list,
        params.num_batches,
        params.count_start
    )

    // Flatten the emitted batch files (so we have individual files)
    batch_files_ch = split_ch.batch_files.flatten()

    // Map each batch file to its batch ID and run ProduceTSVPerSample
    batch_files_ch
        .map { file ->
            // Extract batch ID from filename, e.g., batch_0001 → 0001
            def batch_id = file.getName().replaceAll("batch_", "")
            tuple(batch_id, file)
        }
        .set { batches_with_id_ch }


    // Call the next process using named arguments for everything except the first positional arg
    producetsv_ch = ProduceTSVPerSample(
        batches_with_id_ch.map { it[0] },
        batches_with_id_ch.map { it[1] },
        params.file_gvcf_path,
        params.fasta_ref
    )

    // Flatten the emitted batch files (so we have individual files)
    producetsv_files_ch = producetsv_ch.batch_done.flatten()
    
    // Map each batch file to its batch ID and run ProduceTSVPerSample
    producetsv_files_ch
        .map { file ->
            // Extract batch ID from filename, e.g., batch_0001 → 0001
            def batch_id = file.getName().replaceAll("batch_", "").replaceAll("_gvcf_produced", "")
            tuple(batch_id, file)
        }
        .set { producetsv_files_ch_id }

    MergeTSVParquetByBatch(
        producetsv_files_ch_id.map { it[0] },
        producetsv_files_ch_id.map { it[1] }
    )

    // Collect all parquet files and merge them
    MergeBatches(
        MergeTSVParquetByBatch.out.collect()
    )

    FindUniqShortVariantsVCF(
        MergeBatches.out
    )

    // Split VCF filenames into chrom + file
    vcf_ch = FindUniqShortVariantsVCF.out.chrom_vcfs.flatten()
        .map { file ->
            // extract chromosome name from file, e.g., chr1.vcf.gz → chr1
            def chrom = file.getName().replaceAll("\\.vcf\\.gz\$", "")
            tuple(chrom, file)
        }
    .take(3)  // <-- take only the first 3 chromosomes

    RunVEPDefault(
        vcf_ch.map { it[0] },
        vcf_ch.map { it[1] }
    )

    RunVEPLoftee(
        vcf_ch.map { it[0] },
        vcf_ch.map { it[1] }
    )

    RunVEPAlphamissense(
        vcf_ch.map { it[0] },
        vcf_ch.map { it[1] }
    )

    RunVEPSpliceAI(
        vcf_ch.map { it[0] },
        vcf_ch.map { it[1] }
    )

    // Convert each plugin output to Parquet
    default_parquet_ch = ConvertVEPDefaultParquet('default', RunVEPDefault.out.tsv_vep.collect())
    loftee_parquet_ch = ConvertVEPLofteeParquet('loftee', RunVEPLoftee.out.tsv_vep.collect())
    alphamissense_parquet_ch = ConvertVEPAlphamissenseParquet('alphamissense', RunVEPAlphamissense.out.tsv_vep.collect())
    spliceai_parquet_ch = ConvertVEPSpliceAIParquet('spliceai', RunVEPSpliceAI.out.tsv_vep.collect())

    plugin_parquets_ch = loftee_parquet_ch
        .mix(alphamissense_parquet_ch)
        .mix(spliceai_parquet_ch)
        .collect()


    // Call UnfilteredAnnotation
    UnfilteredAnnotation(
        plugin_parquets_ch,  // tuple of plugin parquets
        default_parquet_ch.plugin_parquet,  // default VEP parquet
        MergeBatches.out              // unannotated ShortVariants parquet
    )

    ProduceSummaryPDF_Unfiltered(
        UnfilteredAnnotation.out
    )

    CuratedAnnotation(
        UnfilteredAnnotation.out
    )

    ProduceSummaryPDF_Curated(
        UnfilteredAnnotation.out
    )
}
