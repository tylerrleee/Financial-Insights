FROM python:3.10-bullseye

# Install Java and system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      openjdk-11-jdk \
      curl \
      wget \
      procps \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Download and install Spark
ENV SPARK_VERSION=3.3.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" ${SPARK_HOME} \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables (fix for PYTHONPATH)
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Create necessary directories
RUN mkdir -p ${SPARK_HOME}/conf \
    && mkdir -p /opt/spark/apps \
    && mkdir -p /opt/spark/data \
    && mkdir -p /opt/spark/spark-events

# Copy configuration files
COPY conf/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

# Set working directory
WORKDIR ${SPARK_HOME}

# Expose ports
EXPOSE 8080 7077 6040

ENTRYPOINT ["/opt/entrypoint.sh"]