FROM flink:1.17.2-scala_2.12

RUN apt-get update && apt-get install -y \
    python3.9 \
    python3-pip \
    default-jre \
    default-jdk \
    gcc \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/flink

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

RUN curl -o /opt/flink/lib/flink-sql-connector-kafka-1.17.2.jar https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar

RUN pip3 install --no-cache-dir apache-flink==1.17.2 kafka-python redis

EXPOSE 8081 6123 6124

ENV FLINK_HOME=/opt/flink
ENV PYTHONPATH=$FLINK_HOME/python
ENV PYFLINK_PYTHON=/usr/bin/python3


RUN ln -s /usr/bin/python3 /usr/bin/python

#COPY device_job.py /opt/flink/
#
#CMD ["sh", "-c", "bin/jobmanager.sh start-foreground & bin/taskmanager.sh start-foreground \
# & ./bin/flink run -py /opt/flink/device_job.py -d \
# && tail -f /dev/null"]
