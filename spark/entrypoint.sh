set -e

if [ "$1" = "master" ]; then
    echo "Starting Spark Master"
    exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master \
        --host 0.0.0.0 \
        --port 7077 \ 
        --webui-port 8080

elif [ "$1" = "worker" ]; then
    echo "Starting Spark Worker"
    exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker \
        spark://spark-master:7077 \
        --memory ${SPARK_WORKER_MEMORY:-2g} \
        --cores ${SPARK_WORKER_CORES:-2}

elif [ "$1" = "history" ]; then
    echo "Starting Spark History Server"
    exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.history.HistoryServer

else
    exec "$@"
fi