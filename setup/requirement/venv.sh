python3 -m venv software/venv
source software/venv/bin/activate
pip install --upgrade pip

pip install pandas psutil tqdm duckdb matplotlib pyspark
pip install snakemake snakemake-executor-plugin-cluster-generic