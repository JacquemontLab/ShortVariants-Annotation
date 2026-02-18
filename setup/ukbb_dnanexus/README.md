

# 🛠️ Setup Instructions

The `INSTALL.sh` script helps you install all the required tools and resources on UKB-RAP.
## These resources need to be made available within your **DNAnexus project**.
By reviewing each step, you can better understand how the Cromwell server accesses the necessary files and resources.

Once you've updated the `String project = "project-XXXXXXXX"` line in `main.wdl` to reflect your project ID, you can compile the workflow from your local machine using:

```bash
java -jar dxCompiler-2.13.0.jar compile main.wdl \
  -project project-XXXXXXXXXXX \
  -folder /ShortVariants_annotation/
```

This will register the WDL as a **workflow** in your UKB-RAP DNAnexus project under the `/ShortVariants_annotation/` folder.

> ✅ You can then launch the workflow directly from the UKB-RAP interface or via the CLI.



# 👤 Bioinformatician Information

**Florian Bénitière**
📅 16/06/2025

## Running a Workflow on UKB-RAP

Running a workflow on the UK Biobank Research Analysis Platform (UKB-RAP) for a large cohort can be technically challenging. The workflow uses the `dxCompiler` language WDL, and I’ve been working on strategies such as **compressing files or directories to efficiently pass them between processes**.
The pipeline was used to process **Whole Exome Sequencing gVCF**.

### Instance Type and Resources

The memory and CPUs used in each step are defined via the `dx_instance_type`, but they can also be managed from the UKB-RAP workflow launcher.
Refer to the instance type documentation here:
[UKB-RAP – Instance Type](https://20779781.fs1.hubspotusercontent-na1.net/hubfs/20779781/Product%20Team%20Folder/Rate%20Cards/BiobankResearchAnalysisPlatform_Rate%20Card_Current.pdf)

For the following steps, ensure that:

* All required data accessed via `dx download` are present in your project. For that it is worth taking a look at the `vep_requirement` directory and the `dockerfiles`, as they will resonate when reading the WDL workflow.
* The `file_gvcf_path` is well configured. It should contain lines formatted as:

```
${sample}\t${input_file_gvcf_path}
```


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


### Step-by-Step Workflow

#### 1. `SplitSampleList`

Splits the list of samples into *X* batches. This step is lightweight and does not require many resources since it only partitions a list.

#### 2. `ProduceTSVPerSampleUKBB`

Downloads scripts, Docker images, and gVCF files to filter per sample using multithreading.
This step is CPU-intensive but doesn't require much memory.
For a batch of 15,651 samples, the runtime was approximately **1h10** with 72 cpus.

#### 3. `MergeTSVParquetByBatch`

Merges all TSV files per batch into Parquet.
This took **2h30** with **16 CPUs**. However, this is a Spark step, and one must be careful about memory configuration. In this case, only **30GB** were provided.

---

### Batch Strategy and Bandwidth Limitation

Running the workflow once causes *X* batches to be processed in parallel, with *X* samples processed concurrently in `ProduceTSVPerSampleUKBB`.
However, this creates severe bandwidth limitations due to too many concurrent `dx download` calls for the gVCFs.

**Solution:** I ran `ProduceTSVPerSampleUKBB` and `MergeTSVParquetByBatch` in **batches of batches**.
I determined that I could download \~432 files simultaneously without issues, so I launched the workflow **5 times**, each on **6 batches** of 15,651 samples.
Total cost: **£6 per batch × 5 = £30** (plus overhead, total \~**£40**).

---

### 4. `MergeBatches`

Once all 30 batches were processed, I merged the resulting Parquet files using `MergeBatches`.
This step required significant memory: **747 GB RAM** and **96 CPUs**, and took **1h30**.
Cost: **£7**

---

### 5. `FindUniqShortVariantsVCF`

This step identifies unique short variants (SNVs and Indels) across individuals for efficient annotation via VEP.
Resources: **96 CPUs**, **747 GB RAM**
Runtime: **2h**
Cost: **£10**

---

### 6. VEP Annotations

To speed up annotation, VEP plugins are run independently and per chromosome.
Each process is multithreaded. The four VEP steps are:

* `RunVEPDefault`: **40 min**, **£3**
* `RunVEPLoftee`: **1h**, **£8**
* `RunVEPAlphamissense`: **1h**, **£5**
* `RunVEPSpliceAI`: **1h30**, **£10**

> Note: The SpliceAI VEP cache is particularly large (\~91 GB).
> These steps did not use excessive memory and ran on instance type `mem1_ssd1_v2_x72` which has **144 GB RAM**.

---

### 7. `ConvertVEPOutParquet`

Each VEP output per chromosome is converted into a single Parquet file.
Resources: **96 CPUs**, **369 GB RAM**
Runtime: **15 min**

---

### 8. `UnFilteredAnnotation`

This is the most resource-intensive step:

* Resources: **96 CPUs**, **747 GB RAM**
* Runtime: **6h30**
* Cost: **£25**

---

### 9. `CuratedAnnotation`

Final annotation step:

* Resources: **96 CPUs**, **747 GB RAM**
* Runtime: **4h**
* Cost: **£14**