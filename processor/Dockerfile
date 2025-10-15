FROM python:3.9-slim-bookworm

# Spark versions
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Spark JAR versions
ENV KAFKA_AVRO_JAR_VERSION=7.5.3
ENV SPARK_AVRO_VERSION=2.4.0
ENV SPARK_SQL_KAFKA_VERSION=3.5.0
ENV COMMONS_POOL_VERSION=2.12.0
ENV SPARK_JARS_DIR=${SPARK_HOME}/jars

# Install dependencies: Java, wget, tar, curl
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    wget \
    tar \
    curl \
 && rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -O spark.tgz && \
    tar -xzf spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark.tgz

# Ensure jars directory exists
RUN mkdir -p ${SPARK_JARS_DIR}

# Download required JARs
# RUN wget -q "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/${KAFKA_AVRO_JAR_VERSION}/kafka-avro-serializer-${KAFKA_AVRO_JAR_VERSION}.jar" -P ${SPARK_JARS_DIR} && \
#     wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/${SPARK_AVRO_VERSION}/spark-avro_2.12-${SPARK_AVRO_VERSION}.jar" -P ${SPARK_JARS_DIR} && \
#     wget -q "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_SQL_KAFKA_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_SQL_KAFKA_VERSION}.jar" -P ${SPARK_JARS_DIR} && \
#     wget -q "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar" -P ${SPARK_JARS_DIR}


# Set working directory
WORKDIR /app

# Install Python dependencies
COPY ./processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./processor/src ./src
COPY ./processor/schemas ./schemas

# Default command to run Spark job
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,io.confluent:kafka-avro-serializer:7.5.3", "--repositories","https://packages.confluent.io/maven/", "src/__main__.py"]




