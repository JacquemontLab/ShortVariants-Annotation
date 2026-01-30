[![Jacquemont's Lab Header](img/labheader.png)](https://www.jacquemont-lab.org/)

[Git Repository ShortVariants-Annotation](https://github.com/JacquemontLab/ShortVariants-Annotation.git)

[![DOI](https://zenodo.org/badge/1021468027.svg)](https://doi.org/10.5281/zenodo.16268986)


# ShortVariants-Annotation

#### A Nextflow pipeline for annotating short variants (SNVs and Indels) on large dataset (>100k vcf) using Ensembl’s Variant Effect Predictor (VEP).

## Overview

This workflow is designed to run on different infrastructures. However, since these environments differ significantly in terms of write permissions, architecture, and security settings, we had to rewrite the workflow and some of the initial scripts, whether using Cromwell or Snakemake.

As a result, three other pipelines are available (see the `setup` directory for details):

* `snakemake` — runs on a **HPC cluster** using Snakemake.
* `ukbb_dnanexus` **UK Biobank (UKBB)** — uses the **UKB-RAP** platform.
* `allofus_workbench` **All of Us** — uses the **All of Us Researcher Workbench** platform - the pipeline closely resembles the UKBB version but skips the first three steps.



## Requirements

Refer to the template config files and adjust them to match your infrastructure.

Required software:

* **Nextflow** – workflow engine (nextflow version 25.10.2)
* **Docker** (Apptainer or Singularity) – to run containers

You might need to pull the following containers if working **offline** (see templates `nextflow.config` or to use slurm `setup/nextflow_HPC_slurm.config`):
* **docker://ghcr.io/jacquemontlab/ensembl_vep_113:latest**
* **docker://ghcr.io/jacquemontlab/pyspark:latest**
* **docker://ghcr.io/jacquemontlab/genomics_tools:latest**
* **docker://ghcr.io/jacquemontlab/duckdb_python:latest**


### Download resources

All required pipeline resources (e.g. reference genome, VEP cache, annotation resources) can be downloaded using the provided setup script:
⚠️ This process can take more than 1 hour.

It will require tabix and samtools installed on the system. The BaseSpace CLI will be installed automatically for downloading SpliceAI resources.

```bash
bash INSTALL.sh
```

This script performs the following tasks:

* Downloads the reference genome (GRCh38 by default)
* Retrieves necessary annotation resources:
  * VEP cache
  * AlphaMissense data
  * LoFTEE plugin
  * SpliceAI scores



## Inputs


| Parameter          | Description                                         | Default    |
| ------------------ | --------------------------------------------------- | ---------- |
| `--dataset_name`   | Name of the dataset, used for directory and report naming.    | dataset_default    |
| `--file_gvcf_path` | A gzip-compressed, tab-separated file that maps each `sampleID` to its corresponding file path, see `Parameter Details`.    | *Required*    |
| `--fasta_ref` | The downloaded `GRCh38_full_analysis_set_plus_decoy_hla.fa.gz`.    | *Required*    |
| `--vep_cache` | The downloaded directory `resources/vep_cache/`.    | *Required*    |
| `--batch_size` | Number of samples call in a single batch.    | 64    |


### Parameter Details

The pipeline works with compressed VCF files (`*vcf.gz`).

#### `sample_to_gvcf.tsv.gz`

A **gzip-compressed, tab-separated file** that maps each `sampleID` to its corresponding file path.

**Example:**

```
sampleID	Path
AXXXXX	    /absolute/path/to/sample_A.vcf.gz
BXXXXX	    /absolute/path/to/sample_B.vcf.gz
CXXXXX	    /absolute/path/to/sample_C.vcf.gz
DXXXXX	    /absolute/path/to/sample_D.vcf.gz
```


## Usage

### Testing

The pipeline can be tested using the test profile and the images hosted on github using the container of your choice.
Unfortunately, the test will fail at the Curated steps because it uses a small test dataset. Those, allele frequency (AF) filtering will remove all the short variants.

```bash
container=docker # or apptainer or singularity

nextflow run main.nf -profile test,${container}
```

### Example

```bash
file_gvcf_path=$PWD/tests/sample_to_gvcf.tsv.gz
fasta_ref=$PWD/resources//reference_genome/GRCh38_full_analysis_set_plus_decoy_hla.fa.gz
vep_cache=$PWD/resources/vep_cache/
container=docker # or apptainer or singularity

nextflow run main.nf \
    --dataset_name Dataset \
    --file_gvcf_path "$file_gvcf_path" \
    --fasta_ref "$fasta_ref" \
    --vep_cache "$vep_cache" \
    --batch_size 16 \
    -profile ${container}
```

Here’s a corrected and cleaner version of your section:

### Running on Compute Canada (CCDB, lab)

Users on CCDB can run the pipeline offline, since the required resources and Docker/Apptainer containers are already available on Rorqual:

```bash
export NXF_OFFLINE=true
module load nextflow
module load apptainer

file_gvcf_path=$PWD/tests/sample_to_gvcf.tsv.gz

nextflow run main.nf \
    --dataset_name Dataset \
    --file_gvcf_path "$file_gvcf_path" \
    -c setup/nextflow_HPC_slurm.config
```




## Outputs

There are two output tables:

### Output format of **`ShortVariantsDB_curated.parquet`**

| Column name | Label | Description |
|------------------|------------------|------------------------------------|
| **SampleID** | Sample ID | Unique identifier for the sample. |
| **CHROM** | Chromosome | Chromosome where the variant is located. |
| **POS** | Position | Genomic position of the variant. |
| **REF** | Reference allele | Reference allele at the variant position. |
| **ALT** | Alternate allele | Alternate allele observed at the variant position. |
| **GT** | Genotype | Genotype of the individual for the variant. |
| **DP** | Read depth | Read depth supporting the variant call. |
| **GQ** | Genotype quality | Genotype quality score. |
| **REF_AD** | Reference allele read depth | Read count supporting the reference allele. |
| **ALT_AD** | Alternate allele read depth | Read count supporting the alternate allele. |
| **AC_ratio** | Allele count ratio | Allele count ratio of the alternate allele. `ALT_AD / DP` |
| **Gene** | Gene Ensembl stable ID | Ensembl stable ID of affected gene. |
| **Feature** | Transcript Ensembl stable ID | Ensembl stable ID of feature. |
| **CANONICAL** | Canonical transcript | Indicates whether the transcript is the canonical transcript. |
| **MANE** | Mane Transcript | Transcript is the MANE Select or MANE Plus Clinical transcript for the gene. |
| **dataset_AF** | Dataset Allele Frequency | Number of individual having the short variant (after QC filter) / 2 × the total number of individuals in the dataset. |
| **gnomAD_max_AF** | Maximum allele frequency across gnomAD | Maximum AF across all gnomAD population (exome and genome). |
| **MAX_AF** | Maximum allele frequency across populations | Maximum observed allele frequency in 1000 Genomes, ESP and gnomAD. |
| **MAX_AF_POPS** | Populations with maximum allele frequency | Population in which was found MAX_AF. |
| **Variant_Type** | Type of variant | Functional impact classification of the variant. |
| **am_pathogenicity** | AlphaMissense pathogenicity score | A continuous score between 0 and 1 which can be interpreted as the predicted probability of the variant being pathogenic, from AlphaMissense plugin. |
| **Max_DS_SpliceAI** | Maximum SpliceAI Delta Score | Maximum SpliceAI delta score for splicing impact. |


### Output format of **`ShortVariantsDB_unfiltered.parquet`**

| Column name | Label | Description |
|-------------|-------|-------------|
| **ID** | Variant ID | Unique identifier of the short variant. |
| **SampleID** | Sample ID | Unique identifier for the sample. |
| **CHROM** | Chromosome | Chromosome where the variant is located. |
| **POS** | Position | Genomic position of the variant. |
| **REF** | Reference allele | Reference allele at the variant position. |
| **ALT** | Alternate allele | Alternate allele observed at the variant position. |
| **GT** | Genotype | Genotype of the individual for the variant. |
| **DP** | Read depth | Read depth supporting the variant call. |
| **GQ** | Genotype quality | Genotype quality score. |
| **REF_AD** | Reference allele read depth | Read count supporting the reference allele. |
| **ALT_AD** | Alternate allele read depth | Read count supporting the alternate allele. |
| **Gene** | Gene Ensembl stable ID | Ensembl stable ID of affected gene. |
| **Feature** | Transcript Ensembl stable ID | Ensembl stable ID of feature. |
| **Consequence** | Variant consequence | Predicted functional consequence of the variant from VEP. |
| **CANONICAL** | Canonical transcript | Indicates whether the transcript is the canonical transcript. |
| **MANE** | MANE transcript | Indicates whether the transcript is a MANE Select or MANE Plus Clinical transcript. |
| **MAX_AF** | Maximum allele frequency across populations | Maximum observed allele frequency in 1000 Genomes, ESP and gnomAD. |
| **MAX_AF_POPS** | Populations with maximum allele frequency | Population in which was found MAX_AF. |
| **gnomAD_max_AF** | Maximum allele frequency across gnomAD | Maximum AF across all gnomAD population (exome and genome). |
| **SYMBOL** | SpliceAI SYMBOL | HGNC gene symbol. |
| **DS_AG** | SpliceAI DS_AG | SpliceAI delta score for acceptor gain. |
| **DS_AL** | SpliceAI DS_AL | SpliceAI delta score for acceptor loss. |
| **DS_DG** | SpliceAI DS_DG | SpliceAI delta score for donor gain. |
| **DS_DL** | SpliceAI DS_DL | SpliceAI delta score for donor loss. |
| **DP_AG** | SpliceAI DP_AG | SpliceAI delta position for acceptor gain. |
| **DP_AL** | SpliceAI DP_AL | SpliceAI delta position for acceptor loss. |
| **DP_DG** | SpliceAI DP_DG | SpliceAI delta position for donor gain. |
| **DP_DL** | SpliceAI DP_DL | SpliceAI delta position for donor loss. |
| **am_class** | AlphaMissense class | AlphaMissense predicted variant class. |
| **am_pathogenicity** | AlphaMissense pathogenicity score | A continuous score between 0 and 1 which can be interpreted as the predicted probability of the variant being pathogenic, from AlphaMissense plugin. |
| **LoF** | Loss-of-function status | LOFTEE loss-of-function annotation. |
| **LoF_filter** | LOFTEE filter | LOFTEE filtering status. |
| **LoF_flags** | LOFTEE flags | LOFTEE flags providing additional annotation context. |
| **LoF_info** | LOFTEE information | Additional LOFTEE annotation details. |






## Different Steps of the Pipeline
This repository provides a workflow implemented in Nextflow (with alternative implementations in Cromwell and Snakemake under `setup`) for processing and annotating short variants (SNVs and Indels) on the human reference genome **GRCh38**.

### Prerequisite to run the pipeline: Downloading the Raw Data

gVCF files need to be available.
We do not use the pVCF files as they contain excessive information that is unnecessary for our purposes.

### 1. Filtering gVCF and taking intersection between callers if multiple are available (e.g., in the case of SPARK)

⚠️**This step may need to be adapted depending on the dataset you are working with**⚠️.
For SPARK, it processes two VCF files per sample and retains only their intersection. In contrast, for UKBB, it processes a single VCF file per sample.


The gVCF files are filtered to retain only:

-   **Non Homozygous reference sites** — Removing `0/0` or `./.` to keep only variant sites.
  
  Some SNPs appear as `1/0` because they were originally multiallelic (e.g., `GT=2/3`) in the raw gVCF. After normalization, each alternate allele is represented separately, resulting in genotypes like `0/1` and `1/0`. As a result, the total depth (`DP`) may differ from the sum of `REF_AD` and `ALT_AD`.

-   **Indels**

-   **SNPs**

This corresponds to the **`ProduceTSVPerSample`** process.
(The **`produce_tsv_per_sample_SPARK`** step in the Snakemake workflow, or to **`ProduceTSVPerSampleUKBB`** in the UKBB Nextflow pipeline.)

### 3. Merging TSV to Parquet

The merging is done in two steps:

1\. Batch merge (**`MergeTSVParquetByBatch`**)

2\. Merge of batches (**`MergeBatches`**)

This approach enables parallelization and significantly increases speed. The output file is **`Unannotated_ShortVariants.parquet`**.

### 4. Identifying Unique short variants (SNVs and Indels)

Unique short variants are extracted to avoid redundant annotation during the VEP step.
The output consists of VCF files per chromosome, used for VEP annotation (**`FindUniqShortVariantsVCF`** step).

### 5. VEP Annotation

Each chromosome's unique short variants (SNVs and Indels) are annotated using **VEP**. Different annotations:

-   **Default VEP annotation**: Consequence, CANONICAL, MANE, MAX_AF, MAX_AF_POPS, gnomADe_, gnomADg_ (**`RunVEPDefault`**)

-   **LoFtee plugin**: LoF, LoF_filter, LoF_flags, LoF_info (**`RunVEPLoftee`**)

-   **AlphaMissense plugin**: am_class, am_pathogenicity scores (**`RunVEPAlphamissense`**)

-   **SpliceAI plugin**: SpliceAI_pred including SYMBOL\|DS_AG\|DS_AL\|DS_DG\|DS_DL\|DP_AG\|DP_AL\|DP_DG\|DP_DL (**`RunVEPSpliceAI`**)

### 6. Reformatting VEP Output

The VEP output is reformatted per plugin, for example **SpliceAI_pred** is split into separate columns, the maximum AF across gnomAD population is computed.
Only unique short variants (SNVs and Indels) with an annotation of the given plugin are retained, reducing file size and computation time (**`ConvertVEPOutParquet`** process).

### 7. Creating an Unfiltered Annotation

This is the most resources consuming step, it merges **`Unannotated_ShortVariants.parquet`** with plugin annotations.
The output is **`ShortVariantsDB_unfiltered.parquet`**, partitioned by chromosome (**`UnFilteredAnnotation`** process).

To reduce the size of the database, we removed:

-   **Short variants for which both MANE and CANONICAL annotations are null**

### 8. Curating Annotation

This step filters data to generate a more relevant downstream dataset (**`CuratedAnnotation`** process). The dataset correspond to **`ShortVariantsDB_curated.parquet`**.

Key processing steps:

-   **Compute Allele Count Ratio**: `AC_ratio = ALT_AD / DP`

-   **Compute Max_DS_SpliceAI**: `max(DS_AG, DS_AL, DS_DG, DS_DL)`

-   **Filter short variants**: - DP \≥ 20 - GQ \≥ 30 - 0.2 \≤ AC_ratio \≤ 0.8

-   **Filter short variants with Allele Frequency below 0.001**: - gnomAD_max_AF \≤ 0.001 and dataset_AF \≤ 0.001

-   **Identify Variant Types**:

if Consequence is `synonymous_variant` → **Synonymous**

if the first consequence is `stop_gained` → **Stop_Gained**

if the first consequence is `frameshift_variant` → **Frameshift**

if the first consequence is `splice_acceptor_variant` or `splice_donor_variant` → **Splice_variants**

if the first consequence is `missense_variant` → **Missense**

-   **Retain Variants Based on Criteria**:

**Synonymous**

**Stop_Gained** (LoF = HC , high-confidence LoF variants)

**Frameshift** (LoF = HC , high-confidence LoF variants)

**Splice_variants** (Max_DS_SpliceAI ≥ 0.8 , 'high precision')

**Missense** (am_pathogenicity ≥ 0.564 , 'likely_pathogenic')

### Workflow DAG
Below is a graphical representation of the workflow:

![Workflow DAG](img/dag.png)


## More Documentation
Here are some useful links about the plugins used in this pipeline, provided by VEP (Variant Effect Predictor).

### DOCKER

We used several Docker containers; the corresponding Dockerfiles are available in the `resources/` directory.

### VEP

[Ensembl VEP Options](https://useast.ensembl.org/info/docs/tools/vep/script/vep_options.html)\
[Ensembl VEP Plugins](https://useast.ensembl.org/info/docs/tools/vep/script/vep_plugins.html)\
[Ensembl VEP Consequences](https://useast.ensembl.org/info/genome/variation/prediction/predicted_data.html)

We used the VEP docker: ensembl-vep release_113.3


## Current Limitations of the pipeline
- Only work for human reference genome version **GRCh38**, but can easly be adaptated for other version.
Indeed the plugin resources used are provided for version **GRCh37** (except LoFtee).

- Resource requirements of each step must be adjusted depending on the quantity of data analyzed.

- The workflow might differ regarding the different platform


