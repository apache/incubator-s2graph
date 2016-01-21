FROM sequenceiq/spark:1.6.0

ARG json=src/test/resources/
COPY target/scala-2.10/*assembly*.jar /tmp
COPY $json /tmp
COPY launch.sh /

ENV JSON $json

CMD ["bash", "launch.sh", "/tmp/$(basename $JSON)"]

