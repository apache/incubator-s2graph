#!/usr/bin/python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os, sys
#, urllib2, urllib

def cleanup(args):
	cmd = "hadoop fs -rm -r /tmp/%s" % args["htable_name"]
	print(cmd)
	ret = os.system(cmd)
	print(cmd, "return", ret)
	return ret

def hfile(args):
	print(args)
	cmd = """HADOOP_CONF_DIR=%s spark-submit --class "org.apache.s2graph.s2jobs.loader.GraphFileGenerator" \
--name "GraphFileGenerator@shon" \
--conf "spark.task.maxFailures=20" \
--conf "spark.executor.extraClassPath=%s" \
--conf "spark.driver.extraClassPath=%s" \
--jars %s \
--master local[2] \
--num-executors %s \
--driver-memory 1g \
--executor-memory 2g \
--executor-cores 1 \
%s \
--input %s \
--tempDir %s \
--output /tmp/%s \
--zkQuorum %s \
--table %s \
--dbUrl '%s' \
--dbUser %s \
--dbPassword %s \
--dbDriver %s \
--maxHFilePerRegionServer %s \
--labelMapping %s \
--autoEdgeCreate %s""" % (args["HADOOP_CONF_DIR"],
						  MYSQL_JAR,
						  MYSQL_JAR,
						  MYSQL_JAR,
						  args["num_executors"],
						  JAR,
						  args["input"],
						  args["tempDir"],
						  args["htable_name"],
						  args["hbase_zk"],
						  args["htable_name"],
						  args["db_url"],
						  args["db_user"],
						  args["db_password"],
						  args["db_driver"],
						  args["max_file_per_region"],
						  args["label_mapping"],
						  args["auto_create_edge"])
	print(cmd)
	ret = os.system(cmd)
	print(cmd, "return", ret)
	return ret

def distcp(args):
	cmd = "hadoop distcp -overwrite -m %s -bandwidth %s /tmp/%s %s/tmp/%s" % (args["-m"], args["-bandwidth"], args["htable_name"], args["hbase_namenode"], args["htable_name"])
	print(cmd)
	ret = os.system(cmd)
	print(cmd, "return", ret)
	return ret

def chmod(args):
	cmd = "export HADOOP_CONF_DIR=%s; export HADOOP_USER_NAME=hdfs; hadoop fs -chmod -R 777 /tmp/%s" % (args["HADOOP_CONF_DIR"], args["htable_name"])
	print(cmd)
	ret = os.system(cmd)
	print(cmd, "return", ret)
	return ret

def load(args):
	cmd = "export HADOOP_CONF_DIR=%s; export HBASE_CONF_DIR=%s; hbase %s /tmp/%s %s" % \
		  (args["HADOOP_CONF_DIR"], args["HBASE_CONF_DIR"], LOADER_CLASS, args["htable_name"], args["htable_name"])
	print(cmd)
	ret = os.system(cmd)
	print(cmd, "return", ret)
	return ret

def send(msg):
	print(msg)

def run(args):
	cleanup(args)
	send("[Start]: bulk loader")
	ret = hfile(args)

	if ret != 0: return send("[Failed]: loader build hfile failed %s" % ret)
	else: send("[Success]: loader build hfile")

	# ret = distcp(args)
	#
	# if ret != 0: return send("[Failed]: loader distcp failed %s" % ret)
	# else: send("[Success]: loader distcp")
	#
	# ret = chmod(args)
	#
	# if ret != 0: return send("[Failed]: loader chmod failed %s" % ret)
	# else: send("[Success]: loader chmod")

	ret = load(args)

	if ret != 0: return send("[Failed]: loader complete bulkload failed %s" % ret)
	else: send("[Success]: loader complete bulkload")


LOADER_CLASS = "org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles"
JAR="s2jobs/target/scala-2.11/s2jobs-assembly-0.2.1-SNAPSHOT.jar"
MYSQL_JAR="/Users/shon/Downloads/mysql-connector-java-5.1.28.jar"
MYSQL_CLASSPATH="/Users/shon/Downloads/mysql-connector-java-5.1.28.jar"
DB_DRIVER="com.mysql.jdbc.Driver"
DB_URL="jdbc:mysql://localhost:3306/graph_dev"
# DB_URL="jdbc:h2:file:./var/metastore;MODE=MYSQL"
args = {
	"HADOOP_CONF_DIR": "/usr/local/Cellar/hadoop/2.7.3/libexec/etc/hadoop",
	"HBASE_CONF_DIR": "/usr/local/opt/hbase/libexec/conf",
	"htable_name": "test",
	"hbase_namenode": "hdfs://localhost:8020",
	"hbase_zk": "localhost",
	"db_driver": DB_DRIVER,
	"db_url": DB_URL,
	"db_user": "graph",
	"db_password": "graph",
	"max_file_per_region": 1,
	"label_mapping": "none",
	"auto_create_edge": "false",
	"-m": 1,
	"-bandwidth": 10,
	"num_executors": 2,
	"input": "/tmp/imei-20.txt",
	"tempDir": "/tmp/bulkload_tmp"
}

run(args)
