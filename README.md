# 🚀 IoT Sensor Data Pipeline – Streaming + Batch on GCP

---

## 📖 The Story Behind This Project

In a world filled with sensors — from smart thermostats to industrial IoT devices — it's critical to ingest, analyze, and act on data **as it arrives**. But in many cases, we also need to process **historical data** stored in files for reporting, reprocessing, or retraining ML models.

This project started with a simple question:

> _"How can we design a single pipeline that handles both real-time and batch IoT data in a scalable and maintainable way?"_

And the answer led to this end-to-end solution on **Google Cloud Platform (GCP)**, combining the power of **Apache Beam** with **Dataflow**, **Pub/Sub**, **BigQuery**, and **Cloud Storage**.

---

## 🎯 Why This Project?

We wanted to build a **unified pipeline** that:

- Supports **streaming** data from Pub/Sub (real-time sensor data)
- Supports **batch** data from GCS (historical logs in CSV format)
- Stores both **raw** and **aggregated** outputs for long-term analytics
- Uses **windowing** to generate time-based summaries (every 1 minute)
- Is **fully serverless**, scalable, and ready for production

---

## ☁️ Why Google Cloud Platform?

GCP offered the perfect stack for this use case:

| GCP Service       | Why We Chose It                                                                 |
|-------------------|---------------------------------------------------------------------------------|
| **Pub/Sub**        | Reliable, scalable ingestion of real-time messages                             |
| **Cloud Storage**  | Cost-effective storage for batch data (CSV files)                              |
| **Dataflow (Beam)**| Unified stream + batch processing logic in one pipeline                        |
| **BigQuery**       | Powerful analytics engine for raw + aggregated data                            |
| **IAM + BQ Schema**| Easy data governance and enforcement of structure                              |

---

## ⚙️ Why This Pipeline Design?

We needed something:

- **Unified** – one pipeline for both sources
- **Real-time ready** – process messages with <1 minute latency
- **Aggregation-friendly** – generate minute-level summaries
- **Cloud-native** – no infra management or scaling headache

So we used **Apache Beam** on **Dataflow**, which let us write a single Python script that could merge two data sources and apply **windowing**, **transformations**, and **writes to BigQuery** — all in a few hundred lines of code.

---

## 🧩 Major Challenges & Optimizations

| Challenge | Solution |
|----------|----------|
| Batch CSV not always available at runtime | Used conditional logic to avoid `.get()` errors on value providers |
| Pub/Sub messages were malformed or missing fields | Added schema validation and default fallbacks |
| Aggregations needed to be grouped correctly per sensor | Used Beam’s `CombinePerKey` and `WindowInto` features |
| Late data or irregular batches | Designed pipeline to be fault-tolerant and streaming-first |

---

## 🧱 Architecture Overview

### 🔄 Full Data Flow

