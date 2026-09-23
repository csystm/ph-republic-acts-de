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

## How to Run
[Placeholder]

## Data Sources
- BetterGov: https://data.bettergov.ph/datasets/24 (CC BY-NC 4.0)
- Lawphil: https://lawphil.net/statutes/repacts/repacts.html

## License
[Placeholder]