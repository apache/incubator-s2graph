#!/bin/bash

usage() {
	echo "Usage: $0 <json>"
}

[ $# -lt 1 ] && { usage; exit 1; }

JSON_FILE=$1

JAR=$(ls -t /tmp/*assembly*.jar | head)
CLASS=org.apache.s2graph.lambda.Launcher
JOB_NAME="s2lambda::$JSON_FILE"

echo ${JAR}
echo ${CLASS}
echo ${JSON_FILE}

hdfs dfsadmin -safemode wait

cat ${JSON_FILE} | xargs -0 spark-submit \
	--class ${CLASS} \
	--driver-memory ${DRIVER_MEMORY} \
	--executor-memory ${EXECUTOR_MEMORY} \
	--executor-cores ${EXECUTOR_CORES} \
	--master ${MASTER} \
	--name ${JOB_NAME} \
	--files ${MYSQL_CONNECTOR_JAR} \
	--driver-class-path $(basename ${MYSQL_CONNECTOR_JAR}) \
	--conf "spark.executor.extraClassPath=$(basename ${MYSQL_CONNECTOR_JAR})" \
	--driver-java-options "-XX:MaxPermSize=512m" \
	--conf "spark.driver.maxResultSize=4g" \
	--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
	--conf "spark.akka.frameSize=100" \
	--conf "spark.executor.extraJavaOptions=-XX:MaxPermSize=512m" \
	${JAR} run
