#!/usr/bin/env bash
source common.sh

q "1. s2graphql is running?"
status=`curl -s -o /dev/null -w "%{http_code}" "$REST"`
if [ $status != '200' ]; then
    warn "s2graphql not running.. "

    cd $ROOTDIR
    output="$(ls target/apache-s2graph-*-incubating-bin)"   
    if [ -z "$output" ]; then
        info "build package..."
        sbt clean package
    fi

    info "now we will launch s2graphql using build scripts"
    cd target/apache-s2graph-*-incubating-bin
    ./bin/start-s2graph.sh s2graphql
else
    info "s2graphql is running!!"
fi 

q "2. s2jobs assembly jar exists?"
jar=`ls $ROOTDIR/s2jobs/target/scala-2.11/s2jobs-assembly*.jar`
if [ -z $jar ]; then
    warn "s2jobs assembly not exists.."
    info "start s2jobs assembly now"
    cd $ROOTDIR
    sbt 'project s2jobs' clean assembly
else
    info "s2jobs assembly exists!!"
fi

q "3. SPARK_HOME is set?"
if [ -z $SPARK_HOME ]; then
    error "it must be setting SPARK_HOME environment variable"
else 
    info "SPARK_HOME exists!! (${SPARK_HOME})"
fi

info "prepare finished.."
