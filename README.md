# 🚀 Crypto Ingestion Pipeline (Azure Functions + SQL + Blob)

## 📌 Project Overview
This project implements a **cloud-native ETL pipeline** to ingest cryptocurrency price data from Yahoo Finance into **Azure SQL Database** and **Azure Blob Storage**.  
It is designed to be **scalable, reliable, and production-ready**, while demonstrating key **data engineering best practices**.

---

## 🎯 Problem
Cryptocurrency prices are highly volatile and updated every second.  
To support analytics, trading strategies, and anomaly detection, organizations need:
- **Reliable incremental ingestion** of time-series data,  
- **Scalable storage** of raw + curated data,  
- **Automated data quality checks**,  
- **Monitoring and observability** for ingestion health.

---

## 💡 Solution
We built a **serverless ETL pipeline** on Azure that:
1. **Ingests data hourly** using **Azure Functions (Timer Trigger)**.  
2. **Fetches data from Yahoo Finance API** (multi-crypto support).  
3. **Stores raw data in Azure Blob** (JSON/Parquet format).  
4. **Inserts curated data into Azure SQL Database** with incremental logic.  
5. **Logs ingestion process & data quality issues** for monitoring.  
6. Runs **automatically and reliably** in the cloud.

---

## ⚙️ Tech Stack
- **Azure Functions** – serverless ingestion (Timer Trigger)  
- **Azure Blob Storage** – raw data lake (Parquet/JSON)  
- **Azure SQL Database** – curated structured storage  
- **Python (pandas, yfinance, pyodbc)** – ETL processing  
- **Logging & Monitoring** – IngestionLog, DataQualityIssues tables  

---

## 🏗️ Architecture
```mermaid
flowchart LR
    subgraph Source["📡 Data Source"]
        YF["Yahoo Finance API"]
    end

    subgraph Ingestion["⚡ Ingestion Layer - Azure Functions"]
        Timer["⏰ Timer Trigger (Hourly)"]
        Fetch["⚡ Parallel Data Fetch"]
        DQ["🔍 Data Quality Validation"]
        Log["📝 Ingestion Logging"]
    end

    subgraph Storage["💾 Data Storage & Processing"]
        Blob["📦 Azure Blob Storage\n(Raw - Parquet/JSON)"]
        SQL["🗄 Azure SQL Database\n(Curated - CryptoPrice)"]
    end

    subgraph Monitoring["📊 Monitoring & Observability"]
        IngestionLog["📝 IngestionLog"]
        DQIssues["⚠️ DataQualityIssues"]
        Dashboard["📊 Power BI / Looker Studio"]
    end

    YF --> Timer
    Timer --> Fetch
    Fetch --> DQ
    DQ --> Blob
    DQ --> SQL
    DQ --> DQIssues
    Log --> IngestionLog
    SQL --> Dashboard
    IngestionLog --> Dashboard
    DQIssues --> Dashboard
