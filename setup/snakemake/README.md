
# 🛠️ Setup Instructions

The `INSTALL.sh` file helps you install all the required tools and resources on your **local machine** or **server**.


# 🚀 Running the Workflow on a Cluster

Once your input file is defined in the `config.json`, you can launch the workflow on a cluster using `snakemake`:

# 👤 Bioinformatician Information

**Florian Bénitière**
📅 12/03/2025



### Inputs Details

The pipeline requires **two input files** to be specified in the `config.json`.
Additionally, the pipeline works with compressed VCF files (`.vcf.gz`) that have been indexed with their corresponding `.vcf.gz.tbi` files.

#### 1. `list_sample.txt`

A plain text file listing the **sample IDs**, one per line.
No header should be included.

**Example:**

```
AXXXXX
BXXXXX
CXXXXX
DXXXXX
```

#### 2. `sample_paths.tsv.gz`

A **gzip-compressed, tab-separated file** that maps each `sampleID` to its corresponding file path.

**Example:**

```
sampleID	Path
AXXXXX	    /absolute/path/to/sample_A.vcf.gz
BXXXXX	    /absolute/path/to/sample_B.vcf.gz
CXXXXX	    /absolute/path/to/sample_C.vcf.gz
DXXXXX	    /absolute/path/to/sample_D.vcf.gz
```

> For the **SPARK\_iwesv3** dataset, each `sampleID` is expected to have **two entries**,
> one with **"deepvariant"** and one with **"gatk"** appearing **anywhere in the absolute path**.

Each sample appears **twice**, with different variant callsets:

```
sampleID	Path
SPARK_001	/data/SPARK_iwesv3/SPARK_001/deepvariant/SPARK_001.deepvariant.vcf.gz
SPARK_001	/data/SPARK_iwesv3/SPARK_001/gatk/SPARK_001.gatk.vcf.gz
SPARK_002	/data/SPARK_iwesv3/SPARK_002/deepvariant/SPARK_002.deepvariant.vcf.gz
SPARK_002	/data/SPARK_iwesv3/SPARK_002/gatk/SPARK_002.gatk.vcf.gz
SPARK_003	/data/SPARK_iwesv3/SPARK_003/deepvariant/SPARK_003.deepvariant.vcf.gz
SPARK_003	/data/SPARK_iwesv3/SPARK_003/gatk/SPARK_003.gatk.vcf.gz
```


> ⚠️ Ensure that every sample listed in `list_sample.txt` has at least one matching path in `sample_paths.tsv.gz`.



### 1. Activate the virtual environment:

```bash
source software/venv/bin/activate
```

### 2. Launch the workflow with Slurm:

```bash
snakemake \
  --snakefile setup/snakemake/Snakefile \
  --configfile setup/snakemake/config.json \
  --executor cluster-generic \
  --cluster-generic-submit-cmd "sbatch --parsable --account=XXXX -N 1 -J {params.name} --ntasks={resources.cpus} --nodes=1 --mem-per-cpu={resources.mem_per_cpu}G -t {resources.time} -o {params.stdout} -e {params.stderr}" \
  --cluster-generic-cancel-cmd "scancel" \
  -j 100 \
  -n \
  --unlock
```

> 💡 Replace `XXXX` with your Slurm account name.
