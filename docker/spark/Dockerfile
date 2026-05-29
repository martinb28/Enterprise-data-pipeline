# Apache Spark + PySpark + Delta Lake + Jupyter
# ARM64 compatible (Apple Silicon M1/M2/M3/M4)
FROM bitnami/spark:3.5

USER root

# Install Python dependencies
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt

# Download Delta Lake and Hadoop AWS JARs for MinIO/S3 connectivity
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar" \
         -o /opt/bitnami/spark/jars/delta-spark_2.12-3.2.0.jar && \
    curl -L "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar" \
         -o /opt/bitnami/spark/jars/delta-storage-3.2.0.jar && \
    curl -L "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
         -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -L "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
         -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Spark default configuration
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

# Set working directory
WORKDIR /opt/spark-apps

USER 1001
