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

import os, sys, urllib2, urllib

def cleanup(args): 
	cmd = "hadoop fs -rm -r /tmp/%s" % args["htable_name"]
	print cmd
	ret = os.system(cmd)
	print cmd, "return", ret
	return ret

def hfile(args):
	cmd = """spark-submit --class "subscriber.TransferToHFile" \
--name "TransferToHFile@shon" \
--conf "spark.task.maxFailures=20" \
--master yarn-cluster \
--num-executors %s --driver-memory 1g --executor-memory 2g --executor-cores 1 %s \
%s /tmp/%s %s %s %s %s %s %s""" % (args["num_executors"], JAR, args["input"], args["htable_name"], args["hbase_zk"], args["htable_name"], args["db_url"], args["max_file_per_region"], args["label_mapping"], args["auto_create_edge"])
	print cmd
	ret = os.system(cmd)
	print cmd, "return", ret
	return ret

def distcp(args): 
	cmd = "hadoop distcp -overwrite -m %s -bandwidth %s /tmp/%s %s/tmp/%s" % (args["-m"], args["-bandwidth"], args["htable_name"], args["hbase_namenode"], args["htable_name"])
	print cmd
	ret = os.system(cmd)
	print cmd, "return", ret
	return ret

def chmod(args):
	cmd = "export HADOOP_CONF_DIR=%s; export HADOOP_USER_NAME=hdfs; hadoop fs -chmod -R 777 /tmp/%s" % (args["HADOOP_CONF_DIR"], args["htable_name"])
	print cmd
	ret = os.system(cmd)
	print cmd, "return", ret
	return ret

def load(args):
	cmd = "export HADOOP_CONF_DIR=%s; export HBASE_CONF_DIR=%s; hbase %s /tmp/%s %s" % (args["HADOOP_CONF_DIR"], args["HBASE_CONF_DIR"], LOADER_CLASS, args["htable_name"], args["htable_name"])
	print cmd
	ret = os.system(cmd)
	print cmd, "return", ret
	return ret

def send(msg):
	print msg

def run(args):
	cleanup(args)
	send("[Start]: bulk loader")
	ret = hfile(args)
	
	if ret != 0: return send("[Failed]: loader build hfile failed %s" % ret)
	else: send("[Success]: loader build hfile")
	
	ret = distcp(args)

	if ret != 0: return send("[Failed]: loader distcp failed %s" % ret)
	else: send("[Success]: loader distcp")

	ret = chmod(args)
	
	if ret != 0: return send("[Failed]: loader chmod failed %s" % ret)
	else: send("[Success]: loader chmod")

	ret = load(args)

	if ret != 0: return send("[Failed]: loader complete bulkload failed %s" % ret)
	else: send("[Success]: loader complete bulkload")


LOADER_CLASS = "org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles"
JAR="loader/target/scala-2.10/s2loader-assembly-0.11.0-SNAPSHOT.jar"


args = {
"HADOOP_CONF_DIR": "hdfs_conf_gasan", 
"HBASE_CONF_DIR": "hbase_conf_gasan", 
"htable_name": "test", 
"hbase_namenode": "hdfs://nameservice:8020",
"hbase_zk": "localhost",
"db_url": "jdbc:mysql://localhost:3306/graph_dev",
"max_file_per_region": 1,
"label_mapping": "none",
"auto_create_edge": "false",
"-m": 1, 
"-bandwidth": 10,
"num_executors": 2,
"input": "/user/test.txt"
}

run(args)
