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

public class ExternalDataConstants {
    // TODO: Remove unused variables.
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
    // specify the HDFS name node address when reading HDFS data
    public static final String KEY_HDFS_URL = "hdfs";
    // specify the path when reading from a file system
    public static final String KEY_PATH = "path";
    // specify the HDFS input format when reading data from HDFS
    public static final String KEY_INPUT_FORMAT = "input-format";
    // specifies the filesystem (localfs or HDFS) when using a filesystem data source
    public static final String KEY_FILESYSTEM = "fs";
    // specifies the address of the HDFS name node
    public static final String KEY_HADOOP_FILESYSTEM_URI = "fs.defaultFS";
    // specifies the class implementation of the accessed instance of HDFS
    public static final String KEY_HADOOP_FILESYSTEM_CLASS = "fs.hdfs.impl";
    public static final String KEY_HADOOP_INPUT_DIR = "mapred.input.dir";
    public static final String KEY_HADOOP_INPUT_FORMAT = "mapred.input.format.class";
    public static final String KEY_HADOOP_SHORT_CIRCUIT = "dfs.client.read.shortcircuit";
    public static final String KEY_HADOOP_SOCKET_PATH = "dfs.domain.socket.path";
    public static final String KEY_HADOOP_BUFFER_SIZE = "io.file.buffer.size";
    public static final String KEY_SOURCE_DATATYPE = "type-name";
    public static final String KEY_DELIMITER = "delimiter";
    public static final String KEY_PARSER_FACTORY = "parser-factory";
    public static final String KEY_DATA_PARSER = "parser";
    public static final String KEY_HEADER = "header";
    public static final String KEY_READER = "reader";
    public static final String KEY_READER_STREAM = "stream";
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
    public static final String KEY_IS_FEED = "is-feed";
    public static final String KEY_WAIT_FOR_DATA = "wait-for-data";
    public static final String KEY_FEED_NAME = "feed";
    // a string representing external bucket name
    public static final String KEY_BUCKET = "bucket";
    // a comma delimited list of nodes
    public static final String KEY_NODES = "nodes";
    // a string representing the password used to authenticate with the external data source
    public static final String KEY_PASSWORD = "password";
    // an integer representing the number of raw records that can be buffered in the parsing queue
    public static final String KEY_QUEUE_SIZE = "queue-size";
    // a comma delimited integers representing the indexes of the meta fields in the raw record (i,e: "3,1,0,2" denotes that the first meta field is in index 3 in the actual record)
    public static final String KEY_META_INDEXES = "meta-indexes";
    // an integer representing the index of the value field in the data type
    public static final String KEY_VALUE_INDEX = "value-index";
    // a string representing the format of the raw record in the value field in the data type
    public static final String KEY_VALUE_FORMAT = "value-format";
    // a boolean indicating whether the feed is a change feed
    public static final String KEY_IS_CHANGE_FEED = "change-feed";
    // a boolean indicating whether the feed use upsert
    public static final String KEY_IS_INSERT_FEED = "insert-feed";
    // an integer representing the number of keys in a change feed
    public static final String KEY_KEY_SIZE = "key-size";
    // a boolean indicating whether the feed produces records with metadata
    public static final String FORMAT_RECORD_WITH_METADATA = "record-with-metadata";
    // a string representing the format of the record (for adapters which produces records with additional information like pk or metadata)
    public static final String KEY_RECORD_FORMAT = "record-format";
    public static final String KEY_META_TYPE_NAME = "meta-type-name";
    public static final String KEY_ADAPTER_NAME = "adapter-name";
    public static final String READER_STREAM = "stream";
    public static final String KEY_HTTP_PROXY_HOST = "http-proxy-host";
    public static final String KEY_HTTP_PROXY_PORT = "http-proxy-port";
    public static final String KEY_HTTP_PROXY_USER = "http-proxy-user";
    public static final String KEY_HTTP_PROXY_PASSWORD = "http-proxy-password";

    /**
     *  Keys for adapter name
     **/
    public static final String KEY_ADAPTER_NAME_TWITTER_PUSH = "twitter_push";
    public static final String KEY_ADAPTER_NAME_PUSH_TWITTER = "push_twitter";
    public static final String KEY_ADAPTER_NAME_TWITTER_PULL = "twitter_pull";
    public static final String KEY_ADAPTER_NAME_PULL_TWITTER = "pull_twitter";
    public static final String KEY_ADAPTER_NAME_TWITTER_USER_STREAM = "twitter_user_stream";
    public static final String KEY_ADAPTER_NAME_LOCALFS = "localfs";
    public static final String KEY_ADAPTER_NAME_SOCKET = "socket";
    public static final String KEY_ALIAS_ADAPTER_NAME_SOCKET = "socket_adapter";
    public static final String KEY_ADAPTER_NAME_HTTP = "http_adapter";

    /**
     * HDFS class names
     */
    public static final String CLASS_NAME_TEXT_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
    public static final String CLASS_NAME_SEQUENCE_INPUT_FORMAT = "org.apache.hadoop.mapred.SequenceFileInputFormat";
    public static final String CLASS_NAME_RC_INPUT_FORMAT = "org.apache.asterix.hivecompat.io.RCFileInputFormat";
    public static final String CLASS_NAME_HDFS_FILESYSTEM = "org.apache.hadoop.hdfs.DistributedFileSystem";
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
    public static final String FORMAT_JSON_LOWER_CASE = "json";
    public static final String FORMAT_JSON_UPPER_CASE = "JSON";
    public static final String FORMAT_DELIMITED_TEXT = "delimited-text";
    public static final String FORMAT_TWEET = "twitter-status";
    public static final String FORMAT_RSS = "rss";
    public static final String FORMAT_SEMISTRUCTURED = "semi-structured";
    public static final String FORMAT_LINE_SEPARATED = "line-separated";
    public static final String FORMAT_HDFS_WRITABLE = "hdfs-writable";
    public static final String FORMAT_KV = "kv";

    /**
     * input streams
     */
    public static final String STREAM_HDFS = "hdfs";
    public static final String STREAM_SOCKET_CLIENT = "socket-client";

    /**
     * adapter aliases
     */
    public static final String ALIAS_GENERIC_ADAPTER = "adapter";
    public static final String ALIAS_LOCALFS_ADAPTER = "localfs";
    public static final String ALIAS_LOCALFS_PUSH_ADAPTER = "push_localfs";
    public static final String ALIAS_HDFS_ADAPTER = "hdfs";
    public static final String ALIAS_SOCKET_CLIENT_ADAPTER = "socket_client";
    public static final String ALIAS_FEED_WITH_META_ADAPTER = "feed_with_meta";
    public static final String ALIAS_CHANGE_FEED_WITH_META_ADAPTER = "change_feed_with_meta";
    // for testing purposes
    public static final String ALIAS_TEST_CHANGE_ADAPTER = "test_change_feed";

    /**
     * Constant String values
     */
    public static final String TRUE = "true";
    public static final String FALSE = "false";

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
    public static final byte BYTE_LF = '\n';
    public static final byte BYTE_CR = '\r';
    /**
     * Size default values
     */
    public static final int DEFAULT_BUFFER_SIZE = 4096;
    public static final float DEFAULT_BUFFER_INCREMENT_FACTOR = 1.5F;
    public static final int DEFAULT_QUEUE_SIZE = 64;
    public static final int MAX_RECORD_SIZE = 32000000;

    /**
     * Expected parameter values
     */
    public static final String PARAMETER_OF_SIZE_ONE = "Value of size 1";
    public static final String LARGE_RECORD_ERROR_MESSAGE = "Record is too large";
    public static final String KEY_RECORD_INDEX = "record-index";
    public static final String FORMAT_DCP = "dcp";
    public static final String KEY_KEY_INDEXES = "key-indexes";
    public static final String KEY_KEY_INDICATORS = "key-indicators";
    public static final String KEY_STREAM_SOURCE = "stream-source";
    public static final String EXTERNAL = "external";
    public static final String KEY_READER_FACTORY = "reader-factory";
    public static final String READER_RSS = "rss_feed";
    public static final String FORMAT_CSV = "csv";

    public static final String ERROR_PARSE_RECORD = "Parser failed to parse record";
}
