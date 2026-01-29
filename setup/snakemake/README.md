
# 🛠️ Setup Instructions

The `INSTALL.sh` file helps you install all the required tools and resources on your **local machine** or **server**.


# 🚀 Running the Workflow on a Cluster

Once your input file is defined in the `config.json`, you can launch the workflow on a cluster using `snakemake`:

# 👤 Bioinformatician Information

**Florian Bénitière**
📅 12/03/2025

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
