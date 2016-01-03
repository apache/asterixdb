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
package org.apache.asterix.external.util;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

public class ExternalDataConstants {
    //TODO: Remove unused variables.
    /**
     * Keys
     */
    // used to specify the stream factory for an adapter that has a stream data source
    public static final String KEY_STREAM = "stream";
    // used to specify the dataverse of the adapter
    public static final String KEY_DATAVERSE = "dataverse";
    // used to specify the socket addresses when reading data from sockets
    public static final String KEY_SOCKETS = "sockets";
    // specify whether the socket address points to an NC or an IP
    public static final String KEY_MODE = "address-type";
    // specify the hdfs name node address when reading hdfs data
    public static final String KEY_HDFS_URL = "hdfs";
    // specify the path when reading from a file system
    public static final String KEY_PATH = "path";
    public static final String KEY_INPUT_FORMAT = "input-format";
    public static final String KEY_FILESYSTEM = "fs";
    public static final String KEY_HADOOP_FILESYSTEM_URI = "fs.defaultFS";
    public static final String KEY_HADOOP_FILESYSTEM_CLASS = "fs.hdfs.impl";
    public static final String KEY_HADOOP_INPUT_DIR = "mapred.input.dir";
    public static final String KEY_HADOOP_INPUT_FORMAT = "mapred.input.format.class";
    public static final String KEY_HADOOP_SHORT_CIRCUIT = "dfs.client.read.shortcircuit";
    public static final String KEY_HADOOP_SOCKET_PATH = "dfs.domain.socket.path";
    public static final String KEY_HADOOP_BUFFER_SIZE = "io.file.buffer.size";
    public static final String KEY_SOURCE_DATATYPE = "type-name";
    public static final String KEY_DELIMITER = "delimiter";
    public static final String KEY_PARSER_FACTORY = "tuple-parser";
    public static final String KEY_DATA_PARSER = "parser";
    public static final String KEY_HEADER = "header";
    public static final String KEY_READER = "reader";
    public static final String KEY_READER_STREAM = "reader-stream";
    public static final String KEY_TYPE_NAME = "type-name";
    public static final String KEY_RECORD_START = "record-start";
    public static final String KEY_RECORD_END = "record-end";
    public static final String KEY_EXPRESSION = "expression";
    public static final String KEY_LOCAL_SOCKET_PATH = "local-socket-path";
    public static final String KEY_FORMAT = "format";
    public static final String KEY_QUOTE = "quote";
    public static final String KEY_PARSER = "parser";
    public static final String KEY_DATASET_RECORD = "dataset-record";
    public static final String KEY_HIVE_SERDE = "hive-serde";
    public static final String KEY_RSS_URL = "url";
    public static final String KEY_INTERVAL = "interval";
    public static final String KEY_PULL = "pull";
    public static final String KEY_PUSH = "push";
    /**
     * HDFS class names
     */
    public static final String CLASS_NAME_TEXT_INPUT_FORMAT = TextInputFormat.class.getName();
    public static final String CLASS_NAME_SEQUENCE_INPUT_FORMAT = SequenceFileInputFormat.class.getName();
    public static final String CLASS_NAME_RC_INPUT_FORMAT = RCFileInputFormat.class.getName();
    public static final String CLASS_NAME_HDFS_FILESYSTEM = DistributedFileSystem.class.getName();
    /**
     * input formats aliases
     */
    public static final String INPUT_FORMAT_TEXT = "text-input-format";
    public static final String INPUT_FORMAT_SEQUENCE = "sequence-input-format";
    public static final String INPUT_FORMAT_RC = "rc-input-format";
    /**
     * Builtin streams
     */

    /**
     * Builtin record readers
     */
    public static final String READER_HDFS = "hdfs";
    public static final String READER_ADM = "adm";
    public static final String READER_SEMISTRUCTURED = "semi-structured";
    public static final String READER_DELIMITED = "delimited-text";

    public static final String CLUSTER_LOCATIONS = "cluster-locations";
    public static final String SCHEDULER = "hdfs-scheduler";
    public static final String PARSER_HIVE = "hive-parser";
    public static final String HAS_HEADER = "has.header";
    public static final String TIME_TRACKING = "time.tracking";
    public static final String DEFAULT_QUOTE = "\"";
    public static final String NODE_RESOLVER_FACTORY_PROPERTY = "node.Resolver";
    public static final String DEFAULT_DELIMITER = ",";
    public static final String EXTERNAL_LIBRARY_SEPARATOR = "#";
    public static final String HDFS_INDEXING_ADAPTER = "hdfs-indexing-adapter";
    /**
     * supported builtin record formats
     */
    public static final String FORMAT_HIVE = "hive";
    public static final String FORMAT_BINARY = "binary";
    public static final String FORMAT_ADM = "adm";
    public static final String FORMAT_JSON = "json";
    public static final String FORMAT_DELIMITED_TEXT = "delimited-text";
    public static final String FORMAT_TWEET = "tweet";
    public static final String FORMAT_RSS = "rss";

    /**
     * input streams
     */
    public static final String STREAM_HDFS = "hdfs";
    public static final String STREAM_LOCAL_FILESYSTEM = "localfs";
    public static final String STREAM_SOCKET = "socket";

    /**
     * adapter aliases
     */
    public static final String ALIAS_GENERIC_ADAPTER = "adapter";
    public static final String ALIAS_LOCALFS_ADAPTER = "localfs";
    public static final String ALIAS_HDFS_ADAPTER = "hdfs";
    public static final String ALIAS_SOCKET_ADAPTER = "socket_adapter";
    public static final String ALIAS_TWITTER_FIREHOSE_ADAPTER = "twitter_firehose";
    public static final String ALIAS_SOCKET_CLIENT_ADAPTER = "socket_client";
    public static final String ALIAS_RSS_ADAPTER = "rss_feed";
    public static final String ALIAS_FILE_FEED_ADAPTER = "file_feed";
    public static final String ALIAS_TWITTER_PUSH_ADAPTER = "push_twitter";
    public static final String ALIAS_TWITTER_PULL_ADAPTER = "pull_twitter";
    public static final String ALIAS_TWITTER_AZURE_ADAPTER = "azure_twitter";
    public static final String ALIAS_CNN_ADAPTER = "cnn_feed";

    /**
     * For backward compatability
     */
    public static final String ADAPTER_LOCALFS_CLASSNAME = "org.apache.asterix.external.dataset.adapter.NCFileSystemAdapter";
    public static final String ADAPTER_HDFS_CLASSNAME = "org.apache.asterix.external.dataset.adapter.HDFSAdapter";

    /**
     * Constant characters
     */
    public static final char ESCAPE = '\\';
    public static final char QUOTE = '"';
    public static final char SPACE = ' ';
    public static final char TAB = '\t';
    public static final char LF = '\n';
    public static final char CR = '\r';
    public static final char DEFAULT_RECORD_START = '{';
    public static final char DEFAULT_RECORD_END = '}';

    /**
     * Constant byte characters
     */
    public static final byte EOL = '\n';
    public static final byte BYTE_CR = '\r';
    /**
     * Size default values
     */
    public static final int DEFAULT_BUFFER_SIZE = 4096;
    public static final int DEFAULT_BUFFER_INCREMENT = 4096;

    /**
     * Expected parameter values
     */
    public static final String PARAMETER_OF_SIZE_ONE = "Value of size 1";

}
