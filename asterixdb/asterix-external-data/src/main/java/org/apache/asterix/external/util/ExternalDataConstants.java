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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.asterix.external.input.record.reader.hdfs.avro.AvroFileInputFormat;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.util.StorageUtil;

public class ExternalDataConstants {

    private ExternalDataConstants() {
    }

    // TODO: Remove unused variables.
    /**
     * Keys
     */
    // used to specify the stream factory for an adapter that has a stream data source
    public static final String KEY_STREAM = "stream";
    //TODO(DB): check adapter configuration
    public static final String KEY_DATASET = "dataset";
    public static final String KEY_DATASET_DATABASE = "dataset-database";
    // used to specify the dataverse of the adapter
    public static final String KEY_DATASET_DATAVERSE = "dataset-dataverse";
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
    //Base64 encoded warnings issued from Hadoop
    public static final String KEY_HADOOP_ASTERIX_WARNINGS_LIST = "org.apache.asterix.warnings.list";
    //Disable caching FileSystem for Hadoop
    public static final String KEY_HADOOP_DISABLE_FS_CACHE_TEMPLATE = "fs.%s.impl.disable.cache";
    //Base64 encoded function call information
    public static final String KEY_HADOOP_ASTERIX_FUNCTION_CALL_INFORMATION = "org.apache.asterix.function.info";
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
    public static final String KEY_SCHEMA = "schema";
    public static final String KEY_PARQUET_ROW_GROUP_SIZE = "row-group-size";
    public static final String PARQUET_DEFAULT_ROW_GROUP_SIZE = "10MB";
    public static final String KEY_PARQUET_PAGE_SIZE = "page-size";
    public static final String PARQUET_DEFAULT_PAGE_SIZE = "8KB";
    public static final String KEY_INCLUDE = "include";
    public static final String KEY_EXCLUDE = "exclude";
    public static final String KEY_QUOTE = "quote";
    public static final String KEY_FORCE_QUOTE = "force-quote";
    public static final String KEY_EMPTY_FIELD_AS_NULL = "empty_field_as_null";
    public static final String KEY_RECORD_DELIMITER = "record-delimiter";
    public static final String KEY_ESCAPE = "escape";
    public static final String KEY_PARSER = "parser";
    public static final String KEY_DATASET_RECORD = "dataset-record";
    public static final String KEY_RSS_URL = "url";
    public static final String KEY_INTERVAL = "interval";
    public static final String KEY_IS_FEED = "is-feed";
    public static final String KEY_LOG_INGESTION_EVENTS = "log-ingestion-events";
    public static final String KEY_WAIT_FOR_DATA = "wait-for-data";
    public static final String KEY_FEED_NAME = "feed";
    // a string representing external bucket name
    public static final String KEY_ENTITY_ID = "entity-id";
    public static final String KEY_EXTERNAL_SOURCE_TYPE = "type";
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
    public static final String TABLE_FORMAT = "table-format";
    public static final String TABLE_METADATA_LOCATION = "metadata-path";
    public static final int SUPPORTED_ICEBERG_FORMAT_VERSION = 1;
    public static final String KEY_META_TYPE_NAME = "meta-type-name";
    public static final String KEY_ADAPTER_NAME = "adapter-name";
    public static final String READER_STREAM = "stream";
    public static final String KEY_HTTP_PROXY_HOST = "http-proxy-host";
    public static final String KEY_HTTP_PROXY_PORT = "http-proxy-port";
    public static final String KEY_HTTP_PROXY_USER = "http-proxy-user";
    public static final String KEY_HTTP_PROXY_PASSWORD = "http-proxy-password";
    // a string representing the NULL value
    public static final String KEY_NULL_STR = "null";
    public static final String KEY_REDACT_WARNINGS = "redact-warnings";
    public static final String KEY_REQUESTED_FIELDS = "requested-fields";
    public static final String KEY_EXTERNAL_SCAN_BUFFER_SIZE = "external-scan-buffer-size";

    public static final String KEY_EMBED_FILTER_VALUES = "embed-filter-values";

    /**
     * Keys for adapter name
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
    public static final String KEY_ADAPTER_NAME_AWS_S3 = "S3";
    public static final String KEY_ADAPTER_NAME_AZURE_BLOB = "AZUREBLOB";
    public static final String KEY_ADAPTER_NAME_AZURE_DATA_LAKE = "AZUREDATALAKE";
    public static final String KEY_ADAPTER_NAME_GCS = "GCS";
    public static final String KEY_ADAPTER_NAME_HDFS = "HDFS";

    public static final Set<String> EXTERNAL_READ_ADAPTERS = Set.of(KEY_ADAPTER_NAME_TWITTER_PUSH,
            KEY_ADAPTER_NAME_PUSH_TWITTER, KEY_ADAPTER_NAME_TWITTER_PULL, KEY_ADAPTER_NAME_PULL_TWITTER,
            KEY_ADAPTER_NAME_TWITTER_USER_STREAM, KEY_ADAPTER_NAME_LOCALFS, KEY_ADAPTER_NAME_SOCKET,
            KEY_ADAPTER_NAME_HTTP, KEY_ADAPTER_NAME_AWS_S3, KEY_ADAPTER_NAME_AZURE_BLOB,
            KEY_ADAPTER_NAME_AZURE_DATA_LAKE, KEY_ADAPTER_NAME_GCS, KEY_ADAPTER_NAME_HDFS);

    /**
     * HDFS class names
     */
    public static final String CLASS_NAME_TEXT_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
    public static final String CLASS_NAME_SEQUENCE_INPUT_FORMAT = "org.apache.hadoop.mapred.SequenceFileInputFormat";
    public static final String CLASS_NAME_PARQUET_INPUT_FORMAT =
            "org.apache.asterix.external.input.record.reader.hdfs.parquet.MapredParquetInputFormat";
    public static final String CLASS_NAME_AVRO_INPUT_FORMAT = AvroFileInputFormat.class.getName();
    public static final String CLASS_NAME_HDFS_FILESYSTEM = "org.apache.hadoop.hdfs.DistributedFileSystem";
    public static final String S3A_CHANGE_DETECTION_REQUIRED = "requireVersionChangeDetection";
    public static final String S3A_CHANGE_DETECTION_REQUIRED_CONFIG_KEY = "fs.s3a.change.detection.version.required";
    public static final String HDFS_IO_COMPRESSION_CODECS_KEY = "io.compression.codecs";
    public static final String HDFS_AVRO_IGNORE_INPUTS_WITHOUT_EXTENSION =
            "avro.mapred.ignore.inputs.without.extension";

    /**
     * input formats aliases
     */
    public static final String INPUT_FORMAT_TEXT = "text-input-format";
    public static final String INPUT_FORMAT_SEQUENCE = "sequence-input-format";
    public static final String INPUT_FORMAT_PARQUET = "parquet-input-format";
    public static final String INPUT_FORMAT_AVRO = "avro-input-format";

    public static final String HDFS_BLOCKSIZE = "blocksize";
    public static final String HDFS_REPLICATION = "replication";
    public static final String HADOOP_AUTHENTICATION = "authentication";
    public static final String KERBEROS_PROTOCOL = "kerberos";
    public static final String KERBEROS_REALM = "realm";
    public static final String KERBEROS_KDC = "kdc";
    public static final String HDFS_USE_DATANODE_HOSTNAME = "use-datanode-hostname";
    public static final String KERBEROS_PRINCIPAL = "principal";
    public static final String KERBEROS_PASSWORD = "password";

    public static final String KEY_HDFS_BLOCKSIZE = "dfs.blocksize";
    public static final String KEY_HDFS_REPLICATION = "dfs.replication";
    public static final String KEY_HADOOP_AUTHENTICATION = "hadoop.security.authentication";
    public static final String KEY_KERBEROS_CONF = "java.security.krb5.conf";
    public static final String KEY_NAMENODE_PRINCIPAL_PATTERN = "dfs.namenode.kerberos.principal.pattern";
    public static final String KEY_HDFS_USE_DATANODE_HOSTNAME = "dfs.client.use.datanode.hostname";
    public static final String KERBEROS_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";
    public static final String KERBEROS_CONFIG_REFRESH = "refreshKrb5Config";
    public static final String KERBEROS_CONFIG_FILE_CONTENT =
            "[libdefaults]\n\tdefault_realm = %1$s\n\n[realms]\n\t%1$s = {\n\t\tkdc = %2$s\n\t}";
    public static final String[] KERBEROS_CONFIG_FILE_PATTERN = { "krb5", ".conf" };

    /**
     * Builtin streams
     */

    /**
     * Builtin record readers
     */
    public static final String READER_HDFS = "hdfs";

    public static final String CLUSTER_LOCATIONS = "cluster-locations";
    public static final String SCHEDULER = "hdfs-scheduler";
    public static final String HAS_HEADER = "has.header";
    public static final String TIME_TRACKING = "time.tracking";
    public static final String DEFAULT_QUOTE = "\"";
    public static final String DEFAULT_SINGLE_QUOTE = "'";
    public static final String NONE = "none";
    public static final String NODE_RESOLVER_FACTORY_PROPERTY = "node.Resolver";
    public static final String DEFAULT_DELIMITER = ",";
    public static final String EXTERNAL_LIBRARY_SEPARATOR = "#";
    public static final String HDFS_INDEXING_ADAPTER = "hdfs-indexing-adapter";
    /**
     * supported builtin record formats
     */
    public static final String FORMAT_BINARY = "binary";
    public static final String FORMAT_ADM = "adm";
    public static final String FORMAT_AVRO = "avro";
    public static final String FORMAT_JSON_LOWER_CASE = "json";
    public static final String FORMAT_CSV_LOWER_CASE = "csv";
    public static final String FORMAT_JSON_UPPER_CASE = "JSON";
    public static final String FORMAT_DELIMITED_TEXT = "delimited-text";
    public static final String FORMAT_TWEET = "twitter-status";
    public static final String FORMAT_RSS = "rss";
    public static final String FORMAT_SEMISTRUCTURED = "semi-structured";
    public static final String FORMAT_LINE_SEPARATED = "line-separated";
    public static final String FORMAT_HDFS_WRITABLE = "hdfs-writable";
    public static final String FORMAT_NOOP = "noop";
    public static final String FORMAT_KV = "kv";
    public static final String FORMAT_CSV = "csv";
    public static final String FORMAT_TSV = "tsv";
    public static final String FORMAT_PARQUET = "parquet";
    public static final String PARQUET_SCHEMA_KEY = "parquet-schema";
    public static final String PARQUET_WRITER_VERSION_KEY = "version";
    public static final String PARQUET_WRITER_VERSION_VALUE_1 = "1";
    public static final String PARQUET_WRITER_VERSION_VALUE_2 = "2";
    public static final String DUMMY_DATABASE_NAME = "dbname";
    public static final String DUMMY_TYPE_NAME = "typeName";
    public static final String DUMMY_DATAVERSE_NAME = "a.b.c";
    public static final String FORMAT_APACHE_ICEBERG = "apache-iceberg";
    public static final String FORMAT_DELTA = "delta";
    public static final Set<String> ALL_FORMATS;
    public static final Set<String> TEXTUAL_FORMATS;

    static {
        ALL_FORMATS = Set.of(FORMAT_BINARY, FORMAT_ADM, FORMAT_JSON_LOWER_CASE, FORMAT_DELIMITED_TEXT, FORMAT_TWEET,
                FORMAT_RSS, FORMAT_SEMISTRUCTURED, FORMAT_LINE_SEPARATED, FORMAT_HDFS_WRITABLE, FORMAT_KV, FORMAT_CSV,
                FORMAT_TSV, FORMAT_PARQUET, FORMAT_AVRO);
        TEXTUAL_FORMATS = Set.of(FORMAT_ADM, FORMAT_JSON_LOWER_CASE, FORMAT_CSV, FORMAT_TSV);
    }

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
    public static final String TAB_STR = "\t";
    public static final String NULL_STR = "\0";

    /**
     * Constant characters
     */
    public static final char ESCAPE = '\\';

    public static final char CSV_ESCAPE = '\"';
    public static final char QUOTE = '"';
    public static final char SPACE = ' ';
    public static final char TAB = '\t';
    public static final char LF = '\n';
    public static final char CR = '\r';
    public static final char DEFAULT_RECORD_START = '{';
    public static final char DEFAULT_RECORD_END = '}';
    public static final char OPEN_BRACKET = '[';
    public static final char CLOSING_BRACKET = ']';
    public static final char COMMA = ',';
    public static final char BYTE_ORDER_MARK = '\uFEFF';

    /**
     * Constant byte characters
     */
    public static final byte BYTE_LF = '\n';
    public static final byte BYTE_CR = '\r';
    /**
     * Size default values
     */
    public static final int DEFAULT_BUFFER_SIZE = StorageUtil.getIntSizeInBytes(8, StorageUtil.StorageUnit.KILOBYTE);
    public static final float DEFAULT_BUFFER_INCREMENT_FACTOR = 1.5F;
    public static final int DEFAULT_QUEUE_SIZE = 64;
    public static final int MAX_RECORD_SIZE = 32000000;

    public static final Supplier<String> EMPTY_STRING = () -> "";
    public static final LongSupplier NO_LINES = () -> -1;

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

    public static final String ERROR_PARSE_RECORD = "Parser failed to parse record";
    public static final String MISSING_FIELDS = "some fields are missing";
    public static final String REC_ENDED_AT_EOF = "malformed input record ended abruptly";
    public static final String EMPTY_FIELD = "empty value";
    public static final String INVALID_VAL = "invalid value";

    public static final String DEFINITION_FIELD_NAME = "definition";
    public static final String CONTAINER_NAME_FIELD_NAME = "container";
    public static final String SUBPATH = "subpath";
    public static final String PREFIX_DEFAULT_DELIMITER = "/";
    public static final Pattern COMPUTED_FIELD_PATTERN = Pattern.compile("\\{[^{}:]+:[^{}:]+}");

    /**
     * Compression constants
     */
    public static final String KEY_COMPRESSION_GZIP = "gzip";
    public static final String KEY_COMPRESSION_SNAPPY = "snappy";
    public static final String KEY_COMPRESSION_ZSTD = "zstd";
    public static final String KEY_COMPRESSION_GZIP_COMPRESSION_LEVEL = "gzipCompressionLevel";

    /**
     * Writer Constants
     */
    public static final String KEY_WRITER_MAX_RESULT = "max-objects-per-file";
    public static final String KEY_VALIDATE_WRITE_PERMISSION = "validate-write-permission";
    public static final String KEY_WRITER_COMPRESSION = "compression";
    public static final int WRITER_MAX_RESULT_DEFAULT = 10000;
    public static final int WRITER_MAX_RESULT_MINIMUM = 1000;
    public static final Set<String> WRITER_SUPPORTED_FORMATS;
    public static final Set<String> WRITER_SUPPORTED_ADAPTERS;
    public static final Set<String> TEXTUAL_WRITER_SUPPORTED_COMPRESSION;
    public static final Set<String> PARQUET_WRITER_SUPPORTED_COMPRESSION;
    public static final Set<String> PARQUET_WRITER_SUPPORTED_VERSION;
    public static final int PARQUET_DICTIONARY_PAGE_SIZE = 1048576;
    public static final List<String> WRITER_SUPPORTED_QUOTES;
    public static final EnumSet<ATypeTag> CSV_WRITER_SUPPORTED_DATA_TYPES =
            EnumSet.of(ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT, ATypeTag.UINT8,
                    ATypeTag.UINT16, ATypeTag.UINT64, ATypeTag.FLOAT, ATypeTag.DOUBLE, ATypeTag.STRING,
                    ATypeTag.BOOLEAN, ATypeTag.DATETIME, ATypeTag.UINT32, ATypeTag.DATE, ATypeTag.TIME);
    public static final String PARQUET_MAX_SCHEMAS_KEY = "max-schemas";
    public static final int PARQUET_MAX_SCHEMAS_DEFAULT_VALUE = 5;
    public static final int PARQUET_MAX_SCHEMAS_MAX_VALUE = 10;

    static {
        WRITER_SUPPORTED_FORMATS = Set.of(FORMAT_JSON_LOWER_CASE, FORMAT_PARQUET, FORMAT_CSV_LOWER_CASE);
        WRITER_SUPPORTED_ADAPTERS = Set.of(ALIAS_LOCALFS_ADAPTER.toLowerCase(), KEY_ADAPTER_NAME_AWS_S3.toLowerCase(),
                KEY_ADAPTER_NAME_GCS.toLowerCase(), KEY_ADAPTER_NAME_HDFS.toLowerCase());
        TEXTUAL_WRITER_SUPPORTED_COMPRESSION = Set.of(KEY_COMPRESSION_GZIP);
        PARQUET_WRITER_SUPPORTED_COMPRESSION =
                Set.of(KEY_COMPRESSION_GZIP, KEY_COMPRESSION_SNAPPY, KEY_COMPRESSION_ZSTD);
        PARQUET_WRITER_SUPPORTED_VERSION = Set.of(PARQUET_WRITER_VERSION_VALUE_1, PARQUET_WRITER_VERSION_VALUE_2);
        WRITER_SUPPORTED_QUOTES = List.of(DEFAULT_QUOTE, DEFAULT_SINGLE_QUOTE, NONE);
    }

    public static class AvroOptions {
        private AvroOptions() {
        }

        // - DECIMAL_TO_DOUBLE: Convert decimal to double (default: false)
        // - UUID_AS_STRING: Convert UUID to string (default: true)
        // - DATE_AS_INT: Convert date to integer (default: true)
        // - TIME_AS_LONG: Convert time to long (default: true)
        // - TIMESTAMP_AS_LONG: Convert timestamp to long (default: true)
        public static final String DECIMAL_TO_DOUBLE = "decimal-to-double";
        public static final String UUID_AS_STRING = "uuid-to-string";
        public static final String DATE_AS_INT = "date-to-int";
        public static final String TIMEZONE = "timezone";
        public static final String TIME_AS_LONG = "time-to-long";
        public static final String TIMESTAMP_AS_LONG = "timestamp-to-long";
    }

    public static class DeltaOptions {
        private DeltaOptions() {
        }

        public static final String DECIMAL_TO_DOUBLE = "decimal-to-double";
        public static final String TIMESTAMP_AS_LONG = "timestamp-to-long";
        public static final String DATE_AS_INT = "date-to-int";
        public static final String TIMEZONE = "timezone";
    }

    public static class ParquetOptions {
        private ParquetOptions() {
        }

        //Prefix for hadoop configurations
        private static final String ASTERIX_HADOOP_PREFIX = "org.apache.asterix.";

        /**
         * Parse Parquet's String JSON type into ADM
         * Default: false
         */
        public static final String PARSE_JSON_STRING = "parse-json-string";
        public static final String HADOOP_PARSE_JSON_STRING = ASTERIX_HADOOP_PREFIX + PARSE_JSON_STRING;

        /**
         * Rebase Decimal and parse it as {@link ATypeTag#DOUBLE}
         * Default: false
         */
        public static final String DECIMAL_TO_DOUBLE = "decimal-to-double";
        public static final String HADOOP_DECIMAL_TO_DOUBLE = ASTERIX_HADOOP_PREFIX + DECIMAL_TO_DOUBLE;

        /**
         * Time Zone ID to convert UTC time and timestamp {@link ATypeTag#TIME} and {@link ATypeTag#DATETIME}
         * Default: ""
         * Note: If a UTC adjusted time and/or timestamp exist in the parquet file, and no time zone id is provided,
         * then we will return the UTC time and issue a warning about that.
         */
        public static final String TIMEZONE = "timezone";
        public static final String HADOOP_TIMEZONE = ASTERIX_HADOOP_PREFIX + TIMEZONE;
    }
}
