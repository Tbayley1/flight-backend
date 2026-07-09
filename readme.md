# ✈️ Real-Time Flight Data Pipeline

This Data Engineering project implements a real-time, event-driven Machine Learning pipeline designed to detect statistically significant market anomalies in flight pricing without relying on batch processing or database polling.

## Executive Summary

**1. Boot & Auto-Heal:** On startup, the Apache Spark processor checks for the pre-trained ML model. If missing, it initiates a self-healing sequence, training a Linear Regression algorithm on historical data to understand how flight features impact pricing.
**2. Ingestion & Clean:** Spark connects to a Kafka message broker, ingesting live JSON flight data in micro-batches. It safely casts strings to decimals and fills missing values to guarantee engine stability.
**3. Inference & Aggregation:** Every live flight passes through the ML model, generating a `predicted_price`. Spark then groups flights by route, calculating the minimum price, average price, standard deviation (volatility), and average AI prediction.
**4. Dual-Brain Upsert & Storage:** Spark connects to PostgreSQL to perform idempotent upserts into the `route_value_stats` table. Simultaneously, it calculates the live Z-Score. Flights dropping into the bottom 2.5% of historical prices (Z ≤ -2.0) are quarantined into `market_anomalies`. All database records and ML models are safely persisted to the host machine via mapped Docker Volumes.
**5. Event Gateway:** Inserting an anomaly triggers an automatic Postgres `NOTIFY` command. A FastAPI microservice catches this event and broadcasts the live deal over a WebSocket to end-users.
**6. Version Control:** A strict `.gitignore` protocol is enforced to ensure massive machine learning binaries, Spark checkpoints, and local database volumes remain securely off the repository.

---

## System Architecture

```mermaid
graph TD
    subgraph "1. Ingestion Layer"
        A[Python Producer] -->|Streams JSON| B(Apache Kafka Broker)
    end
    
    subgraph "2. Processing & ML Layer (Apache Spark)"
        B --> C[Data Cleaning & Casting]
        C --> D{ML Model: Linear Regression}dddd
        D -->|Predicts Price| E[Stateful Aggregation]
        E -->|Calculates Z-Score| F[Dual-Brain Router]
    end
    
    subgraph "3. Storage Layer (PostgreSQL)"
        F -->|Normal Upsert| G[(route_value_stats)]
        F -->|If Z <= -2.0| H[(market_anomalies)]
        H -->|DB Trigger| I((pg_notify event))
    end
    
    subgraph "4. Real-Time API Layer"
        I -->|Listens| J[FastAPI Gateway]
        J -->|Broadcasts JSON| K((WebSockets))
    end
    
    style B fill:#f9f,stroke:#333,stroke-width:2px
    style D fill:#bbf,stroke:#333,stroke-width:2px
    style G fill:#dfd,stroke:#333,stroke-width:2px
    style H fill:#fbb,stroke:#333,stroke-width:2px