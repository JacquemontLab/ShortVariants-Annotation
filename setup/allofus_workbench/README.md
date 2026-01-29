

# 🛠️ Setup Instructions

The `INSTALL.sh` script helps install all the required tools and resources on **All of Us**.
## These resources must be available within your **workspace bucket**.

Also the docker used in the main.wdl needs to be available on Google Cloud Artifact Registry to be used by GCloud.
as described [here](https://support.researchallofus.org/hc/en-us/articles/21179878475028-Using-Docker-Images-on-the-Workbench).


# 👤 Bioinformatician Information

**Florian Bénitière**
📅 29/09/2025

## Running a Workflow on AllOfUs

Running a workflow on the **All of Us Researcher Workbench** for a large cohort can be technically challenging.
The workflow uses the **Cromwell** engine with the **WDL** language. I have been developing strategies such as **compressing files or directories to efficiently pass them between processes**.

I first applied this strategy on **UKB-RAP**, mainly because the files there are stored in an external bucket rather than within the execution environment.



### Instance Type and Resources

The memory and CPU allocation for each step are defined in the `runtime` section of the workflow.


### Step-by-Step Workflow

For **All of Us**, we skip the first three original steps and replace them with a **manual extraction step** using the `Hail Genomics Analysis` environment in Jupyter, which leverages a **Dataproc cluster** for scalability.


#### 1. `from_hail_to_ShortVariants`

We use the Hail table:

```
gs://fc-aou-datasets-controlled/v8/wgs/short_read/snpindel/exome/splitMT/hail.mt
```

as described [here](https://support.researchallofus.org/hc/en-us/articles/29475233432212-Controlled-CDR-Directory).

From this table, we extract the following fields for each variant:
`SampleID`, `CHROM`, `POS`, `REF`, `ALT`, `GT`, `DP`, `GQ`, `REF_AD`, and `ALT_AD`.

This extraction is performed using the script:

```
ShortVariants-Annotation/bin/dataset_specific/from_hail_to_ShortVariants_AllOfUs.py
```

---


### 2. `FindUniqShortVariantsVCF`

This step identifies unique short variants (SNVs and Indels) across individuals for efficient annotation via VEP.
Resources: **96 CPUs**, **384 GB RAM**
Runtime: **1h**

---

### 3. VEP Annotations

To speed up annotation, VEP plugins are run independently and per chromosome.
Each process is multithreaded. The four VEP steps are:

* `RunVEPDefault`: **1h**
* `RunVEPLoftee`: **1h**
* `RunVEPAlphamissense`: **1h**
* `RunVEPSpliceAI`: **2h**

> Note: The SpliceAI VEP cache is particularly large (\~91 GB).
> These steps did not use excessive memory.

---

### 4. `ConvertVEPOutParquet`

Each VEP output per chromosome is converted into a single Parquet file.
Resources: **96 CPUs**, **384 GB RAM**
Runtime: **1h**

---

### 5. `UnFilteredAnnotation`

This is the most resource-intensive step:

* Resources: **96 CPUs**, **624 GB RAM**
* Runtime: **7h**

---

### 6. `CuratedAnnotation`

Final annotation step:

* Resources: **96 CPUs**, **624 GB RAM**
* Runtime: **1h**