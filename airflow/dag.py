from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

import logging
import pytz
import sys
from spark.apps.polygon_fetcher import Financial_Data_Producer

import subprocess

"""
Market Hours Check: Ensures the pipeline only runs during trading hours (9 AM - 4 PM EST, Mon-Fri) as specified in your project
Data Ingestion: Uses your Financial_Data_Producer class to fetch hourly OHLC data from Polygon API 
polygon_fetcher.py
Health Checks: Validates that Kafka and Spark services are healthy before processing
Spark Processing: Submits your anomaly detection job to the Spark cluster 
docker-compose.yml
Data Validation: Ensures data quality and recent data availability
Cleanup: Implements your 7-day rolling deletion policy 
README.md
Alerting: Checks for high-confidence anomalies and logs alerts

1. Check if market is open
2. Check if Kafka and Spark Master/Process is available
3. Fetch data and send to Kafka Producer
4. Send Data from Kafka Consumer into Spark Processing, to populate anomaly variables
5. Check anomalies and send alerts if out of bounds
6. Check for outdated data, and clean up   
"""

# default argugments 
default_args = {
    'owner': 'fininsights',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Inititate DAG
dag = DAG(
    'financial_anomaly_detection_pipeline',
    default_args=default_args,
    description='Real-time financial data pipeline with anomaly detection',
    schedule_interval='0 9-16 * * 1-5',  # Every hour 9 AM - 4 PM EST, Mon-Fri
    catchup=False,
    max_active_runs=1,
    tags=['finance', 'anomaly-detection', 'real-time']
)

def check_market_hours():
    """Check if market is open (9 AM - 4 PM EST, Mon-Fri)"""
    est = pytz.timezone('US/Eastern')
    now = datetime.now(est)
    
    # check if weekday ( 0:Monday, 1:Tuesday,.. 6: Friday)
    if now.weekday() > 4:
      raise Exception("Market is closed - Weekend")
    
    # check if trading hour
    if now.hour < 9 or now.hour > 16:
        raise Exception("Market is closed - Outside trading hours")
    
    logging.info("Market is open - proceeding with data pipeline")

def fetch_and_produce_data():
    """Fetch data from Polygon API and send to Kafka"""
    sys.path.append('/opt/airflow/dags')

    producer = Financial_Data_Producer()
    producer.fetch_hourly_data()
    logging.info("Successfully fetched and sent data to Kafka")

def run_spark_anomaly_detection():
    """Submit Spark job for anomaly detection"""
    spark_submit_cmd = [
        '/opt/spark/bin/spark-submit', # Path to submit binary
        '--master', 'spark://spark-master:7077', # call Spark Master Cluster
        '--conf', 'spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints', # set checkpoint Directory for FAULT TOLERANCE
        '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1',
        '/opt/spark/apps/anomaly_detection_job.py'
    ]
    try:
        result = subprocess.run(
            args = spark_submit_cmd, 
            capture_output = True,
              text = True, 
              check = True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Spark job failed: {e.stderr}")
        raise 

def validate_data_quality():
    """Validate the quality of processed data"""
    postgres_hook = PostgresHook(postgres_conn_id = 'postgres_default')

    recent_data_sql = """
    SELECT COUNT(*) AS recent_count
    FROM financial_data.ohlc_table
    WHERE time_stamp >= NOW() - INTERVAL '2 hours'
    """
    result = postgres_hook.get_first(recent_data_sql)
    recent_count = result[0] if result else 0
    if recent_count == 0:
        raise Exception("No recent data found in the database")
    
    logging.info(f"Data quality check passed: {recent_count} recent records found")

def cleanup_old_data():
    """Clean up data older than 7 days"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    cleanup_sql = """
    DELETE FROM financial_data.ohlc_table 
    WHERE time_stamp < NOW() - INTERVAL '7 days'
    """
    
    postgres_hook.run(cleanup_sql)
    logging.info("Successfully cleaned up data older than 7 days")

def send_anomaly_alerts():
    """Check for anomalies and send alerts if needed"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')

    # Query anomalies
    anomaly_sql = """
    SELECT ticker, anomaly_type, confidence_score, detected_at
    FROM financial_data.anomalies
    WHERE detected_at >= NOW() - INTERVAL '1 hour'
    AND confidence_score > 0.7
"""
    anomalies = postgres_hook.get_records(anomaly_sql)

    if anomalies:
        logging.warning(f"High-confidence anomalies detected: {len(anomalies)} anomalies")
        # Here you could integrate with email, Slack, or other alerting systems
        for anomaly in anomalies:
            logging.warning(f"ANOMALY: {anomaly[0]} - {anomaly[1]} (confidence: {anomaly[2]})")
    else:
        logging.info("No high-confidence anomalies detected")

# Anomalie tasks
check_marketOpen = PythonOperator(
    task_id = 'check_market_hours',
    python_callable=check_market_hours,
    dag=dag
)

# Data producer, sending to Kafka
data_ingestion = PythonOperator(
    task_id = 'fetch_produce_data',
    python_callable=fetch_and_produce_data,
    dag=dag
)

# Process Financial data, updating anomaly variables
spark_processing = PythonOperator(
    task_id = 'run_spark_anomaly_detection',
    python_callable=run_spark_anomaly_detection,
    dag=dag
)

#
data_validation = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag
)

# clean up data after 7 days
data_cleanup = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag
)

# send Alert by checking Anomaly variables
alert_check = PythonOperator(
    task_id='send_anomaly_alerts',
    python_callable=send_anomaly_alerts,
    dag=dag
)

# Health check for Kafka
kafka_health_check = BashOperator(
    task_id='check_kafka_health',
    bash_command='kafka-topics.sh --bootstrap-server kafka:9092 --list',
    dag=dag
)

# Health check for Spark
spark_health_check = BashOperator(
    task_id='check_spark_health',
    bash_command='curl -f http://spark-master:8080 || exit 1',
    dag=dag
)

# Task dependency
check_marketOpen >> [kafka_health_check, spark_health_check]
[kafka_health_check, spark_health_check] >> data_ingestion
data_ingestion >> spark_processing
spark_processing >> data_validation
data_validation >> [data_cleanup, alert_check]




