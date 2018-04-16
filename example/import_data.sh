#!/usr/bin/env bash
source common.sh

[ $# -ne 1 ] && { usage; }

SERVICE=$1
JOBDESC_TPL=${WORKDIR}/${SERVICE}/jobdesc.template
JOBDESC=${WORKDIR}/${SERVICE}/jobdesc.json
WORKING_DIR=`pwd`
JAR=`ls ${ROOTDIR}/s2jobs/target/scala-2.11/s2jobs-assembly*.jar`

info "WORKING_DIR : $WORKING_DIR"
info "JAR : $JAR"

sed -e "s/\[=WORKING_DIR\]/${WORKING_DIR//\//\\/}/g" $JOBDESC_TPL > $JOBDESC

unset HADOOP_CONF_DIR
info "spark submit.."
${SPARK_HOME}/bin/spark-submit \
  --class org.apache.s2graph.s2jobs.JobLauncher \
  --master local[2] \
  --driver-class-path h2-1.4.192.jar \
  $JAR file -f $JOBDESC -n SAMPLEJOB:$SERVICE

