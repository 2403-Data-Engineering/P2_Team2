# P2_Team2

Team 2:
Luke Po,
Henoc Mudibu,
Sai Palepu,
Anand Pawar,
John Wilkins

# Set Up Virtual Environments
1. py -3.11.9 -m venv .venv
2. source/.venv/Scripts/activate
3. pip install -r requirements.txt

# Core Technologies
PySpark — for all data transformation (ingest, clean, enrich, join, feature engineering)
Parquet — storage format for silver and gold layers
Weaviate — local vector database (runs via Docker Compose with text2vec-transformers module for auto-generating embeddings)
Python — for bulk loading and the CLI search script
Docker Compose — to run Weaviate locally
CSV — input data format (messy datasets)

# The Architecture
Messy CSVs (bronze)
  ↓ Spark: clean
Silver Parquet
  ↓ Spark: enrich + join
Gold Parquet
  ↓ Python: bulk load
Weaviate (Docker)
  ↓ Python CLI
Search results