# Real-Time Flight Data Pipeline

This Data Engineering project implements a real-time, event-driven Machine Learning pipeline designed to detect statistically significant market anomalies in flight pricing without relying on batch processing or database polling.

## Summary

**1. Boot & Auto-Heal:** On startup, the Apache Spark processor checks for the pre-trained ML model. If missing, it initiates a self-healing sequence, training a Linear Regression algorithm on historical data to understand how flight features impact pricing.
**2. Ingestion & Clean:** Spark connects to a Kafka message broker, ingesting live JSON flight data in micro-batches. It safely casts strings to decimals and fills missing values to guarantee engine stability.
**3. Inference & Aggregation:** Every live flight passes through the ML model, generating a `predicted_price`. Spark then groups flights by route, calculating the minimum price, average price, standard deviation (volatility), and average AI prediction.
**4. Dual-Brain Upsert & Storage:** Spark connects to PostgreSQL to perform idempotent upserts into the `route_value_stats` table. Simultaneously, it calculates the live Z-Score. Flights dropping into the bottom 2.5% of historical prices (Z ≤ -2.0) are quarantined into `market_anomalies`. All database records and ML models are safely persisted to the host machine via mapped Docker Volumes.
**5. Event Gateway:** Inserting an anomaly triggers an automatic Postgres `NOTIFY` command. A FastAPI microservice catches this event and broadcasts the live deal over a WebSocket to end-users.
**6. Version Control:** A strict `.gitignore` protocol is enforced to ensure massive machine learning binaries, Spark checkpoints, and local database volumes remain securely off the repository.

---
## How to Load and Run the Project

Follow these exact steps to boot the architecture from scratch and watch the real-time anomalies flow.

### 1. Prerequisites
Ensure you have the following installed on your machine:
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Running in the background)
* Python 3.9+ 
* Git

### 2. Clone the Repository
Open your terminal and pull the code to your local machine:
```bash
git clone [https://github.com/Tbayley1/flight-backend.git](https://github.com/Tbayley1/flight-backend.git)
cd flight-backend
