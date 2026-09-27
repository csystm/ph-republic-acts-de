# Philippine Republic Acts Data Engineering Project

## Overview
End-to-end data pipeline for text mining Philippine Republic Acts (RAs) to enable similarity analysis and policy consolidation.

## Problem Statement
The Philippines has over 12,000 Republic Acts with overlapping themes. There is no systematic way to identify similar laws for consolidation. This project builds a reproducible data pipeline that extracts, cleans, validates, and stores the full RA corpus, and then applies text mining to group similar statutes.

## Objectives
- Ingest RA data from BetterGov Parquet and Lawphil HTML.
- Build Raw → Staging → Curated layers with validation.
- Store curated data in PostgreSQL and partitioned Parquet.
- Orchestrate with Apache Airflow in Docker.
- Provide a searchable, analysis-ready corpus.
- (Bonus) Perform similarity analysis and clustering.

## Team
- Francia, L. A.
- Griño, S. R.
- Pesquisa, J.

## Architecture
[Placeholder — will add diagram]

## Repository Structure
[Placeholder]

## Setup and Installation
[Placeholder]

## Configuration

- Locally, `Settings` falls back to `<repo>/data` when `DATA_DIR` is unset.
  Do **not** set `DATA_DIR` in your local `.env`.
- Inside Docker, `docker-compose.yml` sets `DATA_DIR=/opt/airflow/data`,
  which is bind-mounted to `./data`.
- `.env.example` documents the container-side value; keep it in sync
  whenever new env vars are added to `Settings`.

### On committing `ra_master.parquet`

The curated Parquet is checked in alongside the source code. This is a deliberate choice for a course submission — it guarantees the graded artifact is inspectable without rerunning ingestion (which requires network access to HuggingFace and Lawphil). In a production deployment this file would live in object storage (S3/GCS) or be managed via DVC/Git LFS. The 33MB size is well within GitHub's limits.

## How to Run
[Placeholder]

## Data Sources
- BetterGov: https://data.bettergov.ph/datasets/24 (CC BY-NC 4.0)
- Lawphil: https://lawphil.net/statutes/repacts/repacts.html

## License
[Placeholder]