# Global Talent Pulse 🌐

A Real-Time Labor Market Analytics Platform demonstrating high-throughput data ingestion, stream processing, and live visualization using **Apache Kafka**, **Apache Spark (Structured Streaming)**, **Redis**, and **Streamlit**.

## 🏗️ Architecture

The platform follows a **Lambda Architecture** (Hot/Cold paths):
- **Ingestion Layer:** Python scrapers (simulated) push raw job postings to Kafka.
- **Processing Layer (Hot Path):** PySpark Structured Streaming job consumes from Kafka, extracts technology skills from job descriptions using broadcasted taxonomies, and updates live counts in Redis (Global & Regional distribution).
- **Persistence Layer (Cold Path):** Raw enriched data is stored in Parquet format with location-based partitioning for historical batch analysis.
- **Serving Layer:** Redis stores real-time "Pulse" counts and summary metrics.
- **UI Layer:** A rich Streamlit dashboard provides live visualizations, metrics, and distribution charts.

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- 8GB RAM minimum (recommended for Spark/Kafka stack)

### Run the Entire Stack
1. Clone the repository.
2. Navigate to the project root.
3. Start the services:
   ```bash
   docker-compose up --build
   ```

## 📊 Accessing the Platform

| Service | URL |
| :--- | :--- |
| **Streamlit Dashboard** | [http://localhost:8505](http://localhost:8505) |
| **Spark Master UI** | [http://localhost:8085](http://localhost:8085) |
| **Kafka Broker** | `localhost:9095` (External) / `kafka:29092` (Internal) |
| **Redis** | `localhost:6381` (External) / `redis:6379` (Internal) |

## 🛠️ Technical Details & Optimization

- **Multi-Dimensional Aggregation:** The Spark engine tracks both global skill demand and regional distribution (Skill × Location).
- **Redis Pipelines:** Uses Redis pipelines for high-efficiency state updates during `foreachBatch` operations.
- **Resource Management:** Optimized for local development with tuned shuffle partitions (`spark.sql.shuffle.partitions=2`) and memory limits.
- **Persistence:** Implements Structured Streaming with checkpoints for fault-tolerance. Parquet files are partitioned by `location` to optimize downstream analytics.

## 📁 Repository Structure
- `/spark-jobs`: PySpark processing logic and Docker setup.
- `/ingestion`: Python-based data generator mimicing high-velocity job scrapers.
- `/dashboard`: Streamlit real-time monitoring tool.
- `/data`: Local volume for Spark checkpoints and Parquet storage (ignored by Git).

---
*Designed to demonstrate Cloud Engineering and Big Data Pipeline expertise.*
