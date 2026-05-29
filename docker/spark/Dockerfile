# Apache Spark + PySpark + Delta Lake + Jupyter
# ARM64 compatible (Apple Silicon M1/M2/M3/M4)
# Uses official apache/spark image (free, replaces bitnami which went paid)
FROM apache/spark:3.5.5

USER root

# Install system packages + upgrade pip (base image has pip 20, too old)
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3-pip \
        python3-dev \
        curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && pip3 install --upgrade pip setuptools wheel

# Install Python dependencies
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements-spark.txt

# Download Delta Lake and Hadoop AWS JARs for MinIO/S3 connectivity
RUN curl -fL "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar" \
         -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar && \
    curl -fL "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar" \
         -o /opt/spark/jars/delta-storage-3.2.0.jar && \
    curl -fL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
         -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
         -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Spark default configuration
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Set working directory
WORKDIR /opt/spark-apps

# Expose ports: Spark Master UI, Worker UI, Jupyter, Driver
EXPOSE 8080 8081 8888 7077 4040
