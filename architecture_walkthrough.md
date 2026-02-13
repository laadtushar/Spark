# 🏗️ Global Talent Pulse: Architecture Walkthrough

This document provides a deep dive into the technical architecture, data flow, and design decisions behind the Global Talent Pulse platform.

## 1. System Overview

The project implements a **Lambda Architecture** designed to handle high-velocity streaming data while ensuring reliability and persistent storage.

```mermaid
graph TD
    subgraph "Data Ingestion"
        A[Ingestion Generator] -- JSON Events --> B((Apache Kafka))
    end

    subgraph "Processing Layer (Hot Path)"
        B -- Stream Consumption --> C[PySpark Processor]
        C -- Skills Extraction --> C
        C -- Write Aggregates --> D[(Redis)]
    end

    subgraph "Persistence Layer (Cold Path)"
        C -- Write Raw Enriched --> E[Parquet Storage /data]
    end

    subgraph "Serving & UI"
        D -- Query Trends --> F[Streamlit Dashboard]
        F -- Live Visualization --> G[User Browser]
    end

    style B fill:#f96,stroke:#333,stroke-width:2px
    style C fill:#69f,stroke:#333,stroke-width:2px
    style D fill:#f66,stroke:#333,stroke-width:2px
    style F fill:#0f9,stroke:#333,stroke-width:2px
```

---

## 2. Data Flow Sequence

The interaction between components follows a strict event-driven pattern.

```mermaid
sequenceDiagram
    participant G as Job Generator
    participant K as Kafka Broker
    participant S as Spark Engine
    participant R as Redis (Cache)
    participant P as Parquet (Disk)

    G->>K: 1. Produce Job Posting (JSON)
    K->>S: 2. Structured Streaming Micro-batch
    Note over S: 3. Skill Extraction (UDF + Broadcast)
    Note over S: 4. Multi-dim Aggregation (Global/Loc)
    
    par Hot Path
        S->>R: 5a. Pipeline Update Keys (Skill & Location)
    and Cold Path
        S->>P: 5b. Write Partitioned Data (location=X)
    end
    
    Note over R: 6. Metrics available for Dashboard
```

---

## 3. Component Deep Dive

### 🚀 Spark Processing Engine (`processor.py`)
The heart of the system. It uses **PySpark Structured Streaming** with the following technical highlights:

*   **Broadcast Taxonomy:** The `SKILL_TAXONOMY` (Python, Spark, Kafka, etc.) is broadcasted to all executors to ensure high-performance skill matching within a custom Python UDF.
*   **State Management:** Uses `complete` output mode for Hot Path aggregations, ensuring that skill counters are maintained accurately over the duration of the stream.
*   **Redis Pipelines:** To minimize network latency, Spark uses Redis pipelines to push dozens of location-skill updates in a single network round-trip.
*   **Optimized Shuffle:** Configured with `spark.sql.shuffle.partitions = 2` to fit efficiently into a single-node Docker environment.

### 📥 Kafka Ingestion
Acts as the durable buffer. 
*   **Topic:** `job.postings.v1`
*   **Format:** Avro-like JSON containing unique `event_id`, high-resolution `timestamp`, and job `payload`.

### 📊 Dashboard Layer
A Streamlit-based UI that polls Redis every 2 seconds.
*   **Dynamic Discovery:** Uses Redis `KEYS` patterns to automatically detect new skills or locations added by the Spark engine without needing code changes.
*   **Visulization:** Uses **Plotly Express** for high-framerate, interactive charts.

---

## 4. Key Design Decisions

1.  **Why Redis for Serving?** 
    We chose Redis over querying Parquet directly because the dashboard needs millisecond latency for "Live" updates. Redis's atomic increments and hash structures are ideal for real-time counters.
    
2.  **Why Partitioned Parquet?**
    Data is stored as `file:/data/job_postings_pq/location=Berlin, DE/`. This allows downstream Big Data tools (like Hive or Presto) to skip 90% of files when analyzing specific regions, drastically reducing I/O costs.

3.  **Root User in Docker:**
    We explicitly set `user: root` for Spark services to resolve `IOException: Mkdirs failed` errors common when mounting Windows volumes to Linux containers.

4.  **Fault Tolerance:**
    Spark maintains **Checkpoints** in the local `/data` volume. If a container crashes, it resumes exactly where it left off by reading the WAL (Write Ahead Log) from the checkpoint directory.
