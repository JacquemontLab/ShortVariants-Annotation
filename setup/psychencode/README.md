
# 🛠️ Setup Instructions

The `setup.sh` file helps you install all the required tools and resources on your **local machine** or **server**.


# 🚀 Running the Workflow on a Cluster

Once your input file is defined in the `config.json`, you can launch the workflow on a cluster using `snakemake`:

### 1. Activate the virtual environment:

```bash
source software/venv/bin/activate
```

### 2. Launch the workflow with Slurm:

```bash
snakemake \
  --use-apptainer \
  --apptainer-args "--bind /lustre09/project/6008022,/home/flben/links/scratch,/home/flben/" \
  --snakefile setup/psychencode/Snakefile \
  --configfile setup/psychencode/config.json \
  --executor cluster-generic \
  --cluster-generic-submit-cmd "sbatch --parsable --account=XXXX -N 1 -J {params.name} --ntasks={resources.cpus} --nodes=1 --mem-per-cpu={resources.mem_per_cpu}G -t {resources.time} -o {params.stdout} -e {params.stderr}" \
  --cluster-generic-cancel-cmd "scancel" \
  -j 100 \
  -n \
  --unlock
```

> 💡 Replace `XXXX` with your Slurm account name.
