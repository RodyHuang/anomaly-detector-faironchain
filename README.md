# Anomaly Detection on Blockchain Transactions: A Graph-Based Approach within the FairOnChain Infrastructure
An open-source pipeline for **Ethereum token-transfer anomaly detection**, developed within the **FairOnChain**. It covers **ETL → Abstraction → Graph → Features → Detectors → REST API**, with modular code and reproducible paths.

---

## Table of Contents
- [Features](#features)
- [Project Structure](#project-structure)
- [Data Layout](#data-layout)
- [Usage](#usage)
  - [1. Preprocessing](#1-preprocessing)
  - [2. Abstraction](#2-abstraction)
  - [3. Graph Construction](#3-graph-construction)
  - [4. Feature Extraction](#4-feature-extraction)
  - [5. Anomaly Detection](#5-anomaly-detection)
  - [6. API Service](#6-api-service)
- [Monthly Analysis Result Table](#monthly-analysis-result-table)
- [Sample Data](#sample-data)

---

## Features
- **ETL**: Raw → cleaned daily CSVs with lightweight validation.
- **Abstraction**: Monthly tables aligned wiht the **FairOnChain unified data model**
- **Graph**: Monthly **token-transfer** graph (directed; aggregated edges with amount/count).
- **Features**: Node / Motif / Egonet feature sets (+ infra whitelist handling).
- **Detectors**: Rule-based (H1–H6), Statistical (Mahalanobis Distance), Machine-Learning(Isolation Forest).
- **API**: `/v1/top`, `/v1/address`, `/v1/sql` for results exploration.


## Project Structure
etl/            # Preprocessing & abstraction (ETL pipeline)
graph/          # Graph construction & feature extraction
analysis/       # Rule-based, statistical, and ML anomaly detectors
api/            # REST API service
tests/          # Basic ETL checks and evaluation of detector results
data/           # Raw, intermediate, and output data (full datasets excluded; sample files provided)


## Data Layout
The project expects a fixed directory structure for raw, intermediate, and output data.  
Full Ethereum datasets are excluded due to size, but small **sample files** are provided to illustrate the layout.

### Raw daily files (blocks, native transfers)
data/raw/ethereum/
blocks/YYYY/MM/ethereum__blocks__<startBlock>_to_<endBlock>.csv # one file per calendar day
transfers/YYYY/MM/ethereum__native_transfers__<startBlock>_to_<endBlock>.csv # one file per calendar day

### Cleaned daily files
data/intermediate/cleaned/ethereum/
blocks/YYYY/MM/ethereum__blocks__<startBlock>_to_<endBlock>__cleaned.csv
transfers/YYYY/MM/ethereum__native_transfers__<startBlock>_to_<endBlock>__cleaned.csv

### Monthly abstract tables
data/intermediate/abstract/ethereum/YYYY/MM/
ethereum__abstract_block__YYYY_MM.{csv,parquet}
ethereum__abstract_transaction__YYYY_MM.{csv,parquet}
ethereum__abstract_token_transfer__YYYY_MM.{csv,parquet}
ethereum__abstract_account__YYYY_MM.{csv,parquet}
ethereum__abstract_token__YYYY_MM.{csv,parquet}

### Graphs, features, and analysis results
data/output/graph/ethereum/YYYY/MM/ 
ethereum__token_transfer_edgelist__YYYY_MM.parquet
ethereum__token_transfer_graph__YYYY_MM.pkl
ethereum__features__YYYY_MM.{csv,parquet}
ethereum__analysis_result__YYYY_MM.{csv,parquet}

## Usage

### 1. Preprocessing
Standardise raw Ethereum block and transfer data into cleaned daily CSV files.

```bash
python -m etl.run_preprocessing --year 2023 --month 1
```

- **Input**:  
  `data/raw/ethereum/{blocks,transfers}/YYYY/MM/ethereum__*_<startBlock>_to_<endBlock>.csv`
- **Output**:  
  `data/intermediate/cleaned/ethereum/{blocks,transfers}/YYYY/MM/*__cleaned.csv`

### 2. Abstraction
Aggregate cleaned daily files into monthly abstract tables aligned with the FairOnChain schema.

```bash
python -m etl.run_build_abstract --year 2023 --month 1
```

- **Input**:  
  `data/intermediate/cleaned/ethereum/{blocks,transfers}/YYYY/MM/*__cleaned.csv`
- **Output**:  
  `data/intermediate/abstract/ethereum/YYYY/MM/ethereum__abstract_*__YYYY_MM.{csv,parquet}`

### 3. Graph Construction
Build a monthly token-transfer graph and filtered edgelist from the abstract tables.

```bash
python -m graph.run_graph_builder --year 2023 --month 1
```

- **Input**:  
  `data/intermediate/abstract/ethereum/YYYY/MM/ethereum__abstract_*__YYYY_MM.{csv,parquet}`
- **Output**:  
  `data/output/graph/ethereum/YYYY/MM/ethereum__token_transfer_edgelist__YYYY_MM.parquet`
  `data/output/graph/ethereum/YYYY/MM/ethereum__token_transfer_graph__YYYY_MM.pkl`

### 4. Feature Extraction
Extract node, motif, and egonet features from the graph.

```bash
python -m graph.run_feature_extraction --year 2023 --month 1
```

- **Input**:  
  `data/output/graph/ethereum/YYYY/MM/ethereum__token_transfer_graph__YYYY_MM.pkl`
- **Output**:  
  `data/output/graph/ethereum/YYYY/MM/ethereum__features__YYYY_MM.{csv,parquet}`

### 5. Anomaly Detection
Apply rule-based heuristics (H1–H6), Mahalanobis distance, and Isolation Forest.

```bash
python -m analysis.run_anomaly_analysis_pipeline --year 2023 --month 1
```

- **Input**:  
  `data/output/graph/ethereum/YYYY/MM/ethereum__features__YYYY_MM.{csv,parquet}`
- **Output**:  
  `data/output/graph/ethereum/YYYY/MM/ethereum__analysis_result__YYYY_MM.{csv,parquet}`

### 6. API Service
Serve results through a lightweight REST API.

```bash
python -m api.app
```

- **GET /v1/top**: top-N anomalous accounts
```bash
curl "http://127.0.0.1:8000/v1/top?chain=ethereum&year=2023&month=1&n=10"
```
- **GET /v1/address**: anomaly profile of a given address
```bash
curl "http://127.0.0.1:8000/v1/address?chain=ethereum&year=2023&month=1&addr=0x1234567890abcdef..."
```
- **POST /v1/sql**: custom SQL queries on the monthly results
```bash
curl -X POST "http://127.0.0.1:8000/v1/sql" \
     -H "Content-Type: application/json" \
     -d '{
           "chain": "ethereum",
           "year": 2023,
           "month": 1,
           "sql": "SELECT address, final_score_0_100 FROM t ORDER BY final_score_0_100 DESC LIMIT 5;"
         }'
```


## Monthly Analysis Result Table

**This is the central output of the anomaly detection pipeline.** 
All detector outputs are consolidated into a single monthly analysis table:
`ethereum__analysis_result__YYYY_MM.csv`

### Columns
| Category             | Columns |
|----------------------|---------|
| **Identifiers**      | `node`, `address`, `is_infra`, `chain_id`, `year`, `month` |
| **Graph Degrees**    | `in_degree`, `out_degree`, `in_transfer_count`, `out_transfer_count`, `total_input_amount`, `total_output_amount`, `balance_proxy` |
| **Motif Features**   | `self_loop_count`, `two_node_loop_count`, `two_node_loop_amount`, `two_node_loop_tx_count`, `triangle_loop_count`, `triangle_loop_amount`, `triangle_loop_tx_count` |
| **Egonet Features**  | `egonet_node_count`, `egonet_edge_count`, `egonet_density` |
| **Heuristic Flags**  | `H1_flag`, `H1_description`, `H2_flag`, `H2_description`, `H3_flag`, `H3_description`, `H4_flag`, `H4_description`, `H5_flag`, `H5_description`, `H6_flag`, `H6_description` |
| **Statistical / ML Detectors** | `mahalanobis_distance`, `iforest_score` |
| **Scores**           | `rule_score_raw`, `rule_score_100`, `mahalanobis_distance_stats_score_100`, `iforest_stats_score_100`, `final_score_0_100`, `final_score_top_percent`, `final_score_top_percent_display` |


This table is the **data source for the API and SQL service** and the entry point for any downstream
analysis (e.g. dashboards, compliance monitoring, cross-chain studies).

### Example Query

```sql
SELECT address, final_score_0_100, final_score_top_percent
FROM t
WHERE H1_flag = 1
ORDER BY final_score_0_100 DESC
LIMIT 10;
```

## Sample Data

The repository includes **sample Ethereum data from January 2023**.  
Since the full dataset is too large, only a **small subset** is provided to demonstrate the pipeline.  

- **Raw sample files**: `data/raw/ethereum/**/2023/01/`  
- **Intermediate outputs** after ETL and abstraction: `data/intermediate/**/ethereum/2023/01/`  
- **Final results**, including the analysis table:  
  `data/output/**/2023/01/ethereum__analysis_result__2023_01.csv`  

These files let you check the **expected format, schema, and output columns** without running the full dataset. Because the raw data is only a **small representative sample**, the outputs are **illustrative only** and do not reflect full Ethereum network behaviour.
