/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class GraphHFileOutputFormat extends HFileOutputFormat2 {
    private static final String COMPRESSION_FAMILIES_CONF_KEY =
            "hbase.hfileoutputformat.families.compression";
    private static final String BLOOM_TYPE_FAMILIES_CONF_KEY =
            "hbase.hfileoutputformat.families.bloomtype";
    private static final String BLOCK_SIZE_FAMILIES_CONF_KEY =
            "hbase.mapreduce.hfileoutputformat.blocksize";
    private static final String DATABLOCK_ENCODING_FAMILIES_CONF_KEY =
            "hbase.mapreduce.hfileoutputformat.families.datablock.encoding";

    // This constant is public since the client can modify this when setting
    // up their conf object and thus refer to this symbol.
    // It is present for backwards compatibility reasons. Use it only to
    // override the auto-detection of datablock encoding.
    public static final String DATABLOCK_ENCODING_OVERRIDE_CONF_KEY =
            "hbase.mapreduce.hfileoutputformat.datablock.encoding";

    static Log LOG = LogFactory.getLog(GraphHFileOutputFormat.class);

    public static void configureIncrementalLoad(Job job, List<ImmutableBytesWritable> startKeys,
                                                List<String> familyNames, Compression.Algorithm compression, BloomType bloomType,
                                                int blockSize, DataBlockEncoding dataBlockEncoding) throws IOException {

        Configuration conf = job.getConfiguration();

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        // Based on the configured map output class, set the correct reducer to properly
        // sort the incoming values.
        // TODO it would be nice to pick one or the other of these formats.
        if (KeyValue.class.equals(job.getMapOutputValueClass())) {
            job.setReducerClass(KeyValueSortReducer.class);
        } else if (Put.class.equals(job.getMapOutputValueClass())) {
            job.setReducerClass(PutSortReducer.class);
        } else if (Text.class.equals(job.getMapOutputValueClass())) {
            job.setReducerClass(TextSortReducer.class);
        } else {
            LOG.warn("Unknown map output value type:" + job.getMapOutputValueClass());
        }

        conf.setStrings("io.serializations", conf.get("io.serializations"),
                MutationSerialization.class.getName(), ResultSerialization.class.getName(),
                KeyValueSerialization.class.getName());

        job.setNumReduceTasks(startKeys.size());

        configurePartitioner(job, startKeys);
        // Set compression algorithms based on column families
        configureCompression(familyNames, compression, conf);
        configureBloomType(familyNames, bloomType, conf);
        configureBlockSize(familyNames, blockSize, conf);
        configureDataBlockEncoding(familyNames, dataBlockEncoding, conf);

        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.initCredentials(job);
        LOG.info("Incremental table output configured.");
    }

    static void configureCompression(List<String> familyNames, Compression.Algorithm compression,
                                     Configuration conf) throws IOException {
        StringBuilder compressionConfigValue = new StringBuilder();
        int i = 0;
        for (String familyName : familyNames) {
            if (i++ > 0) {
                compressionConfigValue.append('&');
            }
            compressionConfigValue.append(URLEncoder.encode(familyName, "UTF-8"));
            compressionConfigValue.append('=');
            compressionConfigValue.append(URLEncoder.encode(compression.getName(), "UTF-8"));
        }
        // Get rid of the last ampersand
        conf.set(COMPRESSION_FAMILIES_CONF_KEY, compressionConfigValue.toString());
    }

    static void configureBloomType(List<String> familyNames, BloomType bloomType, Configuration conf)
            throws IOException {
        StringBuilder bloomTypeConfigValue = new StringBuilder();
        int i = 0;
        for (String familyName : familyNames) {
            if (i++ > 0) {
                bloomTypeConfigValue.append('&');
            }
            bloomTypeConfigValue.append(URLEncoder.encode(familyName, "UTF-8"));
            bloomTypeConfigValue.append('=');
            String bloomTypeStr = bloomType.toString();
            if (bloomTypeStr == null) {
                bloomTypeStr = HColumnDescriptor.DEFAULT_BLOOMFILTER;
            }
            bloomTypeConfigValue.append(URLEncoder.encode(bloomTypeStr, "UTF-8"));
        }
        conf.set(BLOOM_TYPE_FAMILIES_CONF_KEY, bloomTypeConfigValue.toString());
    }

    static void configureBlockSize(List<String> familyNames, int blockSize, Configuration conf)
            throws IOException {
        StringBuilder blockSizeConfigValue = new StringBuilder();
        int i = 0;
        for (String familyName : familyNames) {
            if (i++ > 0) {
                blockSizeConfigValue.append('&');
            }
            blockSizeConfigValue.append(URLEncoder.encode(familyName, "UTF-8"));
            blockSizeConfigValue.append('=');
            blockSizeConfigValue.append(URLEncoder.encode(String.valueOf(blockSize), "UTF-8"));
        }
        // Get rid of the last ampersand
        conf.set(BLOCK_SIZE_FAMILIES_CONF_KEY, blockSizeConfigValue.toString());
    }

    static void configureDataBlockEncoding(List<String> familyNames, DataBlockEncoding encoding,
                                           Configuration conf) throws IOException {
        StringBuilder dataBlockEncodingConfigValue = new StringBuilder();
        int i = 0;
        for (String familyName : familyNames) {
            if (i++ > 0) {
                dataBlockEncodingConfigValue.append('&');
            }
            dataBlockEncodingConfigValue.append(URLEncoder.encode(familyName, "UTF-8"));
            dataBlockEncodingConfigValue.append('=');
            if (encoding == null) {
                encoding = DataBlockEncoding.NONE;
            }
            dataBlockEncodingConfigValue.append(URLEncoder.encode(encoding.toString(), "UTF-8"));
        }
        conf.set(DATABLOCK_ENCODING_FAMILIES_CONF_KEY, dataBlockEncodingConfigValue.toString());
    }

}
