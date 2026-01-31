

// This rule converts VEP annotation output from compressed TSV format to Parquet, optimizing storage and retrieval.
process ConvertVEPOutParquet {
    label "pyspark"
    tag "Convert VEP output to Parquet: ${plugin}"

    input:
    val plugin
    path vep_tsv_files  // all TSVs for this plugin

    output:
    path "plugins/${plugin}.parquet", emit: plugin_parquet

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
    
    mkdir -p vep_annotation/

    mv *.tsv.gz vep_annotation/

    mkdir -p plugins/

    echo "Converting VEP output for plugin: ${plugin} to Parquet"
    /opt/spark/bin/spark-submit --driver-memory \${MEM_GB}g \
        /usr/bin/convert_vep_output_parquet.py \
        vep_annotation/ \
        plugins/${plugin}.parquet \
        ${task.cpus} \
        \${MEM_PER_CPU_GB} \
        ${plugin} 2>&1 | tee output.log
    """
}




// This rule produces a filtered parquet file commonly used for downstream analysis.
process ProduceSummaryPDF {
    label "duckdb_python"

    // Inputs
    input:
    path ShortVariants_annotated_parquet

    // Outputs
    output:
    path "${ShortVariants_annotated_parquet.baseName}_dictionary.pdf", emit: pdf_file

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

    timedev -v /usr/bin/pdf_dictionnary.py \
        ${ShortVariants_annotated_parquet} \
        ${task.cpus} \
        \${MEM_PER_CPU_GB}
    """
}
