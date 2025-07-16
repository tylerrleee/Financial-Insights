# FinInsightsDash - Real-Time Financial Anomaly Detection Pipeline (in-production as of 07/15)
<div ">

Python
Docker
Apache Spark
Apache Kafka
PostgreSQL
Apache Airflow

A production-ready financial data pipeline for real-time anomaly detection in stock market data

</div>

## Overview
FinInsightsDash is a comprehensive financial data pipeline that fetches real-time OHLC (Open, High, Low, Close) data from Polygon.io API, processes it through Apache Kafka and Apache Spark for advanced anomaly detection, and provides actionable insights through a real-time dashboard. The entire system is containerized using Docker and orchestrated with Apache Airflow.

## Key Features
- **Near Real-time Data Processing**: Hourly OHLC data ingestion with 4 API calls/minute rate limiting (Polygon free tier)
- **Anomaly Detection**:** Multi-dimensional analysis including price volatility, volume z-scores, price gaps, high-low ratios, and volume-price trends
- **Scalable Architecture**: Handles up to 3GB daily data volume with auto-scaling Spark workers
- **Automated Scheduling**: Runs every trading hour (9 AM - 4 PM EST, Mon-Fri) via Airflow
- **Data Retention Management**:** Automated 7-day rolling deletion policy
- **Real-time Dashboard**: Live anomaly monitoring and alerting system
- **Production-Ready**: Full containerization with Docker Compose

## System Architecture
<p align="center">
  <a href="Diagram.png" class="image fit">
    <img src="Diagram.png" 

  </a>
</p>

## Tech Stack

## Anomaly Detection Capabilities
The system detects multiple types of financial anomalies with ML-powered confidence scoring:

### Supported Anomaly Types
- Price Volatility (>5% threshold): Unusual price movement patterns
- Volume Z-Score (>3 std dev): Abnormal trading volume spikes/drops
- Price Gaps (>2%): Significant gaps between closing and opening prices
- Extreme Movements (>8%): Rapid hourly price changes
- High-Low Ratios (>10%): Unusual intraday trading ranges
- Volume-Price Divergence: Negative correlation between volume and price trends
- Composite Anomalies: Multiple simultaneous anomalies with boosted confidence
### Machine Learning Features
-Multi-dimensional Feature Engineering: 7+ technical indicators
- Real-time Scoring: Sub-30 second anomaly detection
- Confidence Levels: 0.0-1.0 confidence scoring for each anomaly
- Historical Context: 24-hour and 7-day rolling windows for comparison
