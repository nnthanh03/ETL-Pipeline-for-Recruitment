FROM bitnami/spark:latest

USER root
RUN apt-get update && apt-get install -y openjdk-11-jdk wget && rm -rf /var/lib/apt/lists/*

RUN wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar -P /opt/spark/jars/

RUN wget https://downloads.datastax.com/cpp-driver/ubuntu/ubuntu-20/cassandra-driver-3.1.0.tar.gz -P /opt/spark/jars/

ENV SPARK_CLASSPATH="/opt/spark/jars/mysql-connector-java-8.0.33.jar:/opt/spark/jars/cassandra-driver-3.1.0.tar.gz"

WORKDIR /app
COPY main.py /app/main.py

CMD ["spark-submit", "--packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,com.mysql:mysql-connector-j:8.0.33", "/app/main.py"]
