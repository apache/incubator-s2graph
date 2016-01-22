FROM sequenceiq/spark:1.6.0

ARG json=src/test/resources/pi.json
COPY target/scala-2.10/*assembly*.jar /tmp
COPY $json /tmp
COPY launch.sh /

ENV JSON $json
ENV MASTER yarn-client
ENV DRIVER_MEMORY 1g
ENV EXECUTOR_MEMORY 1g
ENV EXECUTOR_CORES 1
ENV MYSQL_CONNECTOR_VERSION 5.1.28
ENV MYSQL_CONNECTOR_JAR mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar

RUN curl -LO "http://central.maven.org/maven2/mysql/mysql-connector-java/${MYSQL_CONNECTOR_VERSION}/${MYSQL_CONNECTOR_JAR}"

CMD ["bash", "launch.sh", "/tmp/$(basename $JSON)"]

