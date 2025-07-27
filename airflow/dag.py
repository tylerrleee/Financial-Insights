from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
import requests
import logging
import pytz
import sys
from kafka import KafkaConsumer
import json
from spark.apps.polygon_fetcher import Financial_Data_Producer
import subprocess
from spark.ticker_inputs import requested_tickers

# Default arguments
default_args = {
    'owner': 'fininsights',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='financial_anomaly_detection_pipeline',
    default_args=default_args,
    description='Real-time financial data pipeline with anomaly detection',
    schedule='0 9-16 * * 1-5',  # Every hour 9 AM - 4 PM EST, Mon-Fri
    catchup=False,
    max_active_runs=1,
    tags=['finance', 'anomaly-detection', 'real-time']
)
def financial_anomaly_detection_dag():
    """
    Financial Anomaly Detection Pipeline DAG
    
    Workflow:
    1. Pre-flight checks (market hours, service health)
    2. Data ingestion from Polygon API to Kafka
    3. Spark processing for anomaly detection
    4. Data validation and quality checks
    5. Post-processing (cleanup, alerts)
    """
    
    @task_group(group_id='pre_flight_checks')
    def pre_flight_checks():
        """Pre-flight checks before data processing"""
        
        @task
        def check_market_hours():
            """Check if market is open (9 AM - 4 PM EST, Mon-Fri)"""
            est = pytz.timezone('US/Eastern')
            now = datetime.now(est)
            
            # Check if weekday (0=Monday, 6=Sunday)
            if now.weekday() > 4:
                raise Exception("Market is closed - Weekend")
            
            # Check if within market hours
            if now.hour < 9 or now.hour >= 16:
                raise Exception("Market is closed - Outside trading hours")
            
            logging.info("Market is open - proceeding with data pipeline")
            return {"status": "market_open", "timestamp": now.isoformat()}
        
        @task
        def check_kafka_health():
            """Check Kafka service health"""
            try:
                result = subprocess.run(
                    ['kafka-topics.sh', '--bootstrap-server', 'kafka:9092', '--list'],
                    capture_output=True, text=True, check=True, timeout=30
                )
                logging.info("Kafka service is healthy")
                return {"status": "healthy", "topics": result.stdout.strip().split('\n')}
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
                raise Exception(f"Kafka health check failed: {e}")
        
        @task
        def check_spark_health():
            """Check Spark cluster health"""
            try:
                response = requests.get('http://spark-master:8080', timeout=30)
                response.raise_for_status()
                logging.info("Spark cluster is healthy")
                return {"status": "healthy", "master_url": "spark://spark-master:7077"}
            except requests.RequestException as e:
                raise Exception(f"Spark health check failed: {e}")
        
        # Execute pre-flight checks
        market_status = check_market_hours()
        kafka_status = check_kafka_health()
        spark_status = check_spark_health()
        
        return {
            'market': market_status,
            'kafka': kafka_status,
            'spark': spark_status
        }
    
    @task_group(group_id='data_ingestion')
    def data_ingestion():
        """Data ingestion from Polygon API to Kafka"""
        
        @task
        def fetch_polygon_data():
            """Fetch data from Polygon API and send to Kafka"""
            sys.path.append('/opt/airflow/dags')
                        
            producer = Financial_Data_Producer()
            try:
                producer.fetch_hourly_data()
                logging.info("Successfully fetched and sent data to Kafka")
                return {"status": "success", "tickers": producer.major_tickers}
            except Exception as e:
                logging.error(f"Data fetch failed: {e}")
                raise
        
        @task
        def validate_kafka_messages():
            """Validate messages were successfully sent to Kafka"""
            
            consumer = KafkaConsumer(
                'financial_ohlc',
                bootstrap_servers='kafka:9092',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=10000
            )
            
            message_count = 0
            for message in consumer:
                message_count += 1
                if message_count >= 10:  # Check first 10 messages
                    break
            
            consumer.close()
            
            if message_count == 0:
                raise Exception("No messages found in Kafka topic")
            
            logging.info(f"Validated {message_count} messages in Kafka")
            return {"message_count": message_count}
        
        # Execute data ingestion tasks
        polygon_data = fetch_polygon_data()
        kafka_validation = validate_kafka_messages()
        
        return {
            'polygon': polygon_data,
            'kafka_validation': kafka_validation
        }
    
    @task_group(group_id='spark_processing')
    def spark_processing():
        """Spark processing for anomaly detection"""
        
        @task
        def submit_anomaly_detection_job():
            """Submit Spark job for anomaly detection"""
            spark_submit_cmd = [
                '/opt/spark/bin/spark-submit',
                '--master', 'spark://spark-master:7077',
                '--conf', 'spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints',
                '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1',
                '--driver-memory', '1g',
                '--executor-memory', '2g',
                '/opt/spark/apps/anomaly_detection_job.py'
            ]
            
            try:
                result = subprocess.run(
                    spark_submit_cmd, 
                    capture_output=True, 
                    text=True, 
                    check=True,
                    timeout=600  # 10 minute timeout
                )
                logging.info(f"Spark job completed successfully")
                return {
                    "status": "success",
                    "stdout": result.stdout[-1000:],  # Last 1000 chars
                    "execution_time": "completed"
                }
            except subprocess.CalledProcessError as e:
                logging.error(f"Spark job failed: {e.stderr}")
                raise Exception(f"Spark anomaly detection job failed: {e.stderr}")
            except subprocess.TimeoutExpired:
                raise Exception("Spark job timed out after 10 minutes")
        
        @task
        def monitor_spark_job():
            """Monitor Spark job execution"""
            import requests
            import time
            
            # Check Spark UI for recent applications
            try:
                response = requests.get('http://spark-master:8080/json', timeout=10)
                spark_info = response.json()
                
                active_apps = spark_info.get('activeapps', [])
                completed_apps = spark_info.get('completedapps', [])
                
                logging.info(f"Active Spark applications: {len(active_apps)}")
                logging.info(f"Recently completed applications: {len(completed_apps)}")
                
                return {
                    "active_apps": len(active_apps),
                    "completed_apps": len(completed_apps),
                    "cluster_status": "healthy"
                }
            except Exception as e:
                logging.warning(f"Could not fetch Spark cluster info: {e}")
                return {"cluster_status": "unknown"}
        
        # Execute Spark processing tasks
        job_result = submit_anomaly_detection_job()
        monitoring = monitor_spark_job()
        
        return {
            'job_result': job_result,
            'monitoring': monitoring
        }
    
    @task_group(group_id='data_validation')
    def data_validation():
        """Comprehensive data quality validation"""
        
        @task
        def validate_data_quality():
            """Enhanced data quality validation"""
            postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
            
            # 1. Recency Check
            recency_sql = """
            SELECT COUNT(*) as recent_count
            FROM financial_data.ohlc_table
            WHERE time_stamp >= NOW() - INTERVAL '2 hours'
            """
            result = postgres_hook.get_first(recency_sql)
            recent_count = result[0] if result else 0
            
            if recent_count == 0:
                raise Exception("No recent data found in database")
            
            # 2. Completeness Check
            expected_tickers = requested_tickers
            completeness_sql = """
            SELECT ticker, COUNT(*) as record_count
            FROM financial_data.ohlc_table
            WHERE time_stamp >= NOW() - INTERVAL '1 hour'
            GROUP BY ticker
            """
            ticker_results = postgres_hook.get_records(completeness_sql)
            found_tickers = {row[0] for row in ticker_results}
            missing_tickers = set(expected_tickers) - found_tickers
            
            if missing_tickers:
                raise Exception(f"Missing data for tickers: {missing_tickers}")
            
            # 3. Data Integrity Check
            integrity_sql = """
            SELECT COUNT(*) as invalid_ohlc
            FROM financial_data.ohlc_table
            WHERE time_stamp >= NOW() - INTERVAL '2 hours'
            AND (high < low OR high < open OR high < close OR 
                 low > open OR low > close OR
                 open <= 0 OR high <= 0 OR low <= 0 OR close <= 0 OR volume < 0)
            """
            invalid_result = postgres_hook.get_first(integrity_sql)
            invalid_count = invalid_result[0] if invalid_result else 0
            
            if invalid_count > 0:
                raise Exception(f"Found {invalid_count} records with invalid OHLC data")
            
            logging.info(f"Data quality validation passed:")
            logging.info(f"  - Recent records: {recent_count}")
            logging.info(f"  - All expected tickers present: {expected_tickers}")
            logging.info(f"  - No invalid OHLC relationships found")
            
            return {
                "recent_count": recent_count,
                "expected_tickers": expected_tickers,
                "found_tickers": list(found_tickers),
                "invalid_count": invalid_count
            }
        
        @task
        def validate_anomaly_detection():
            """Validate anomaly detection results"""
            postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
            
            # Check if anomaly detection ran successfully
            anomaly_sql = """
            SELECT COUNT(*) as processed_records
            FROM financial_data.ohlc_table
            WHERE time_stamp >= NOW() - INTERVAL '1 hour'
            AND (volume > 0 OR close > 0)  -- Basic processing check
            """
            
            result = postgres_hook.get_first(anomaly_sql)
            processed_count = result[0] if result else 0
            
            logging.info(f" Anomaly detection validation: {processed_count} records processed")
            
            return {"processed_records": processed_count}
        
        # Execute validation tasks
        quality_check = validate_data_quality()
        anomaly_check = validate_anomaly_detection()
        
        return {
            'quality': quality_check,
            'anomaly_validation': anomaly_check
        }
    
    @task_group(group_id='post_processing')
    def post_processing():
        """Post-processing tasks: cleanup and alerts"""
        
        @task
        def cleanup_old_data():
            """Clean up data older than 7 days"""
            postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
            
            cleanup_sql = """
            DELETE FROM financial_data.ohlc_table 
            WHERE time_stamp < NOW() - INTERVAL '7 days'
            """
            
            deleted_count = postgres_hook.run(cleanup_sql)
            logging.info(f"Cleaned up old data: {deleted_count} records removed")
            
            return {"deleted_records": deleted_count}
        
        
        # Execute post-processing tasks
        cleanup_result = cleanup_old_data()
        
        #alert_result = send_anomaly_alerts()
        
        return {
            'cleanup': cleanup_result,
        }
    
    # Define task dependencies using TaskGroups
    preflight = pre_flight_checks()
    ingestion = data_ingestion()
    processing = spark_processing()
    validation = data_validation()
    postprocess = post_processing()
    
    # Set task group dependencies
    preflight >> ingestion >> processing >> validation >> postprocess

# Instantiate the DAG
financial_dag = financial_anomaly_detection_dag()


