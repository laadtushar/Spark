# Global Talent Pulse 🌐

A Real-Time Labor Market Analytics Platform demonstrating high-throughput data ingestion, stream processing, and live visualization using **Apache Kafka**, **Apache Spark (Structured Streaming)**, **Redis**, and **Streamlit**.

## 🏗️ Architecture

The platform follows a **Lambda Architecture** (Hot/Cold paths):
- **Ingestion Layer:** Python scrapers (simulated) push raw job postings to Kafka.
- **Processing Layer (Hot Path):** PySpark Structured Streaming job consumes from Kafka, extracts technology skills from job descriptions using broadcasted taxonomies, and updates live counts in Redis.
- **Persistence Layer (Cold Path):** Raw enriched data is stored in Parquet/Delta Lake format for historical batch analysis.
- **Serving Layer:** Redis stores the real-time "Pulse" counts.
- **UI Layer:** A Streamlit dashboard provides live visualizations of skill adoption trends.

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.9+ (optional, if running components outside Docker)

### Run the Entire Stack
1. Clone the repository.
2. Navigate to the project root:
   ```bash
   cd Global-Talent-Pulse
   ```
3. Start the services:
   ```bash
   docker-compose up --build
   ```

## 📊 Accessing the Platform

| Service | URL |
| :--- | :--- |
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) |
| **Spark Master UI** | [http://localhost:8080](http://localhost:8080) |
| **Kafka Broker** | `localhost:9092` |
| **Redis** | `localhost:6379` |

## 🛠️ Technical Details

- **Skill Extraction:** Uses a high-performance UDF with broadcast variables to scan job descriptions for 10+ core tech stacks (Python, Spark, Kafka, AWS, etc.).
- **Watermarking:** Implements watermarking on event timestamps to handle late-arriving data in the streaming pipeline.
- **Metadata-Driven:** The ingestion layer and processing taxonomy can be updated via configuration/metadata, showcasing architectural flexibility.

---
*Designed to demonstrate Cloud Engineering and Big Data Pipeline expertise.*
