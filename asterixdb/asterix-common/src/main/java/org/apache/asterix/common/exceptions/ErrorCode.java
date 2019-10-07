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
package org.apache.asterix.common.exceptions;

import java.io.InputStream;
import java.util.Map;

import org.apache.hyracks.api.util.ErrorMessageUtil;

// Error code:
// 0 --- 999:  runtime errors
// 1000 ---- 1999: compilation errors
// 2000 ---- 2999: storage errors
// 3000 ---- 3999: feed errors
// 4000 ---- 4999: lifecycle management errors
public class ErrorCode {
    private static final String RESOURCE_PATH = "asx_errormsg/en.properties";
    public static final String ASTERIX = "ASX";

    // Runtime errors
    public static final int CASTING_FIELD = 1;
    public static final int TYPE_MISMATCH_FUNCTION = 2;
    public static final int TYPE_INCOMPATIBLE = 3;
    public static final int TYPE_UNSUPPORTED = 4;
    public static final int TYPE_ITEM = 5;
    public static final int INVALID_FORMAT = 6;
    public static final int OVERFLOW = 7;
    public static final int UNDERFLOW = 8;
    public static final int INJECTED_FAILURE = 9;
    public static final int NEGATIVE_VALUE = 10;
    public static final int OUT_OF_BOUND = 11;
    public static final int COERCION = 12;
    public static final int DUPLICATE_FIELD_NAME = 13;
    public static final int PROPERTY_NOT_SET = 14;
    public static final int ROOT_LOCAL_RESOURCE_EXISTS = 15;
    public static final int ROOT_LOCAL_RESOURCE_COULD_NOT_BE_CREATED = 16;
    public static final int UNKNOWN_EXTERNAL_FILE_PENDING_OP = 17;
    public static final int TYPE_CONVERT = 18;
    public static final int TYPE_CONVERT_INTEGER_SOURCE = 19;
    public static final int TYPE_CONVERT_INTEGER_TARGET = 20;
    public static final int TYPE_CONVERT_OUT_OF_BOUND = 21;
    public static final int FIELD_SHOULD_BE_TYPED = 22;
    public static final int NC_REQUEST_TIMEOUT = 23;
    public static final int POLYGON_INVALID_COORDINATE = 24;
    public static final int POLYGON_3_POINTS = 25;
    public static final int POLYGON_INVALID = 26;
    public static final int OPERATION_NOT_SUPPORTED = 27;
    public static final int INVALID_DURATION = 28;
    public static final int UNKNOWN_DURATION_UNIT = 29;
    public static final int REQUEST_TIMEOUT = 30;
    public static final int INVALID_TYPE_CASTING_MATH_FUNCTION = 31;
    public static final int REJECT_BAD_CLUSTER_STATE = 32;
    public static final int REJECT_NODE_UNREGISTERED = 33;
    public static final int UNSUPPORTED_MULTIPLE_STATEMENTS = 35;
    public static final int CANNOT_COMPARE_COMPLEX = 36;
    public static final int TYPE_MISMATCH_GENERIC = 37;
    public static final int DIFFERENT_LIST_TYPE_ARGS = 38;
    public static final int INTEGER_VALUE_EXPECTED = 39;
    public static final int NO_STATEMENT_PROVIDED = 40;
    public static final int REQUEST_CANCELLED = 41;
    public static final int TPCDS_INVALID_TABLE_NAME = 42;
    public static final int VALUE_OUT_OF_RANGE = 43;
    public static final int PROHIBITED_STATEMENT_CATEGORY = 44;
    public static final int INTEGER_VALUE_EXPECTED_FUNCTION = 45;

    public static final int UNSUPPORTED_JRE = 100;

    public static final int EXTERNAL_UDF_RESULT_TYPE_ERROR = 200;

    // Compilation errors
    public static final int PARSE_ERROR = 1001;
    public static final int COMPILATION_TYPE_MISMATCH_FUNCTION = 1002;
    public static final int COMPILATION_TYPE_INCOMPATIBLE = 1003;
    public static final int COMPILATION_TYPE_UNSUPPORTED = 1004;
    public static final int COMPILATION_TYPE_ITEM = 1005;
    public static final int COMPILATION_DUPLICATE_FIELD_NAME = 1006;
    public static final int COMPILATION_INVALID_EXPRESSION = 1007;
    public static final int COMPILATION_INVALID_PARAMETER_NUMBER = 1008;
    public static final int COMPILATION_INVALID_RETURNING_EXPRESSION = 1009;
    public static final int COMPILATION_FULLTEXT_PHRASE_FOUND = 1010;
    public static final int COMPILATION_UNKNOWN_DATASET_TYPE = 1011;
    public static final int COMPILATION_UNKNOWN_INDEX_TYPE = 1012;
    public static final int COMPILATION_ILLEGAL_INDEX_NUM_OF_FIELD = 1013;
    public static final int COMPILATION_FIELD_NOT_FOUND = 1014;
    public static final int COMPILATION_ILLEGAL_INDEX_FOR_DATASET_WITH_COMPOSITE_PRIMARY_INDEX = 1015;
    public static final int COMPILATION_INDEX_TYPE_NOT_SUPPORTED_FOR_DATASET_TYPE = 1016;
    public static final int COMPILATION_FILTER_CANNOT_BE_NULLABLE = 1017;
    public static final int COMPILATION_ILLEGAL_FILTER_TYPE = 1018;
    public static final int COMPILATION_CANNOT_AUTOGENERATE_COMPOSITE_PRIMARY_KEY = 1019;
    public static final int COMPILATION_ILLEGAL_AUTOGENERATED_TYPE = 1020;
    public static final int COMPILATION_PRIMARY_KEY_CANNOT_BE_NULLABLE = 1021;
    public static final int COMPILATION_ILLEGAL_PRIMARY_KEY_TYPE = 1022;
    public static final int COMPILATION_CANT_DROP_ACTIVE_DATASET = 1023;
    public static final int COMPILATION_AQLPLUS_IDENTIFIER_NOT_FOUND = 1024;
    public static final int COMPILATION_AQLPLUS_NO_SUCH_JOIN_TYPE = 1025;
    public static final int COMPILATION_FUNC_EXPRESSION_CANNOT_UTILIZE_INDEX = 1026;
    public static final int COMPILATION_DATASET_TYPE_DOES_NOT_HAVE_PRIMARY_INDEX = 1027;
    public static final int COMPILATION_UNSUPPORTED_QUERY_PARAMETER = 1028;
    public static final int NO_METADATA_FOR_DATASET = 1029;
    public static final int SUBTREE_HAS_NO_DATA_SOURCE = 1030;
    public static final int SUBTREE_HAS_NO_ADDTIONAL_DATA_SOURCE = 1031;
    public static final int NO_INDEX_FIELD_NAME_FOR_GIVEN_FUNC_EXPR = 1032;
    public static final int NO_SUPPORTED_TYPE = 1033;
    public static final int NO_TOKENIZER_FOR_TYPE = 1034;
    public static final int INCOMPATIBLE_SEARCH_MODIFIER = 1035;
    public static final int UNKNOWN_SEARCH_MODIFIER = 1036;
    public static final int COMPILATION_BAD_QUERY_PARAMETER_VALUE = 1037;
    public static final int COMPILATION_ILLEGAL_STATE = 1038;
    public static final int COMPILATION_TWO_PHASE_LOCKING_VIOLATION = 1039;
    public static final int DATASET_ID_EXHAUSTED = 1040;
    public static final int INDEX_ILLEGAL_ENFORCED_NON_OPTIONAL = 1041;
    public static final int INDEX_ILLEGAL_NON_ENFORCED_TYPED = 1042;
    public static final int INDEX_RTREE_MULTIPLE_FIELDS_NOT_ALLOWED = 1043;
    public static final int REMOTE_EXCEPTION_WHEN_CALLING_METADATA_NODE = 1044;
    public static final int ILLEGAL_LOCK_UPGRADE_OPERATION = 1045;
    public static final int ILLEGAL_LOCK_DOWNGRADE_OPERATION = 1046;
    public static final int UPGRADE_FAILED_LOCK_WAS_NOT_ACQUIRED = 1047;
    public static final int DOWNGRADE_FAILED_LOCK_WAS_NOT_ACQUIRED = 1048;
    public static final int LOCK_WAS_ACQUIRED_DIFFERENT_OPERATION = 1049;
    public static final int UNKNOWN_DATASET_IN_DATAVERSE = 1050;
    public static final int INDEX_ILLEGAL_ENFORCED_ON_CLOSED_FIELD = 1051;
    public static final int INDEX_ILLEGAL_REPETITIVE_FIELD = 1052;
    public static final int CANNOT_CREATE_SEC_PRIMARY_IDX_ON_EXT_DATASET = 1053;
    public static final int COMPILATION_FAILED_DUE_TO_REPLICATE_OP = 1054;
    public static final int COMPILATION_INCOMPATIBLE_FUNCTION_LANGUAGE = 1055;
    public static final int TOO_MANY_OPTIONS_FOR_FUNCTION = 1056;
    public static final int EXPRESSION_NOT_SUPPORTED_IN_CONSTANT_RECORD = 1057;
    public static final int LITERAL_TYPE_NOT_SUPPORTED_IN_CONSTANT_RECORD = 1058;
    public static final int UNSUPPORTED_WITH_FIELD = 1059;
    public static final int WITH_FIELD_MUST_BE_OF_TYPE = 1060;
    public static final int WITH_FIELD_MUST_CONTAIN_SUB_FIELD = 1061;
    public static final int CONFIGURATION_PARAMETER_INVALID_TYPE = 1062;
    public static final int UNKNOWN_DATAVERSE = 1063;
    public static final int ERROR_OCCURRED_BETWEEN_TWO_TYPES_CONVERSION = 1064;
    public static final int CHOSEN_INDEX_COUNT_SHOULD_BE_GREATER_THAN_ONE = 1065;
    public static final int CANNOT_SERIALIZE_A_VALUE = 1066;
    public static final int CANNOT_FIND_NON_MISSING_SELECT_OPERATOR = 1067;
    public static final int CANNOT_GET_CONDITIONAL_SPLIT_KEY_VARIABLE = 1068;
    public static final int CANNOT_DROP_INDEX = 1069;
    public static final int METADATA_ERROR = 1070;
    public static final int DATAVERSE_EXISTS = 1071;
    public static final int DATASET_EXISTS = 1072;
    public static final int UNDEFINED_IDENTIFIER = 1073;
    public static final int AMBIGUOUS_IDENTIFIER = 1074;
    public static final int FORBIDDEN_SCOPE = 1075;
    public static final int NAME_RESOLVE_UNKNOWN_DATASET = 1076;
    public static final int NAME_RESOLVE_UNKNOWN_DATASET_IN_DATAVERSE = 1077;
    public static final int COMPILATION_UNEXPECTED_OPERATOR = 1078;
    public static final int COMPILATION_ERROR = 1079;
    public static final int UNKNOWN_NODEGROUP = 1080;
    public static final int UNKNOWN_FUNCTION = 1081;
    public static final int UNKNOWN_TYPE = 1082;
    public static final int UNKNOWN_INDEX = 1083;
    public static final int INDEX_EXISTS = 1084;
    public static final int TYPE_EXISTS = 1085;
    public static final int PARAMETER_NO_VALUE = 1086;
    public static final int COMPILATION_INVALID_NUM_OF_ARGS = 1087;
    public static final int FIELD_NOT_FOUND = 1088;
    public static final int FIELD_NOT_OF_TYPE = 1089;
    public static final int ARRAY_FIELD_ELEMENTS_MUST_BE_OF_TYPE = 1090;
    public static final int COMPILATION_TYPE_MISMATCH_GENERIC = 1091;
    public static final int ILLEGAL_SET_PARAMETER = 1092;
    public static final int COMPILATION_TRANSLATION_ERROR = 1093;
    public static final int RANGE_MAP_ERROR = 1094;
    public static final int COMPILATION_EXPECTED_FUNCTION_CALL = 1095;
    public static final int UNKNOWN_COMPRESSION_SCHEME = 1096;
    public static final int UNSUPPORTED_WITH_SUBFIELD = 1097;
    public static final int COMPILATION_INVALID_WINDOW_FRAME = 1098;
    public static final int COMPILATION_UNEXPECTED_WINDOW_FRAME = 1099;
    public static final int COMPILATION_UNEXPECTED_WINDOW_EXPRESSION = 1100;
    public static final int COMPILATION_UNEXPECTED_WINDOW_ORDERBY = 1101;
    public static final int COMPILATION_EXPECTED_WINDOW_FUNCTION = 1102;
    public static final int COMPILATION_ILLEGAL_USE_OF_IDENTIFIER = 1103;
    public static final int INVALID_FUNCTION_MODIFIER = 1104;
    public static final int OPERATION_NOT_SUPPORTED_ON_PRIMARY_INDEX = 1105;
    public static final int EXPECTED_CONSTANT_VALUE = 1106;
    public static final int UNEXPECTED_HINT = 1107;

    // Feed errors
    public static final int DATAFLOW_ILLEGAL_STATE = 3001;
    public static final int UTIL_DATAFLOW_UTILS_TUPLE_TOO_LARGE = 3002;
    public static final int UTIL_DATAFLOW_UTILS_UNKNOWN_FORWARD_POLICY = 3003;
    public static final int OPERATORS_FEED_INTAKE_OPERATOR_DESCRIPTOR_CLASSLOADER_NOT_CONFIGURED = 3004;
    public static final int PARSER_DELIMITED_NONOPTIONAL_NULL = 3005;
    public static final int PARSER_DELIMITED_ILLEGAL_FIELD = 3006;
    public static final int ADAPTER_TWITTER_TWITTER4J_LIB_NOT_FOUND = 3007;
    public static final int OPERATORS_FEED_INTAKE_OPERATOR_NODE_PUSHABLE_FAIL_AT_INGESTION = 3008;
    public static final int FEED_CREATE_FEED_DATATYPE_ERROR = 3009;
    public static final int PARSER_HIVE_NON_PRIMITIVE_LIST_NOT_SUPPORT = 3010;
    public static final int PARSER_HIVE_FIELD_TYPE = 3011;
    public static final int PARSER_HIVE_GET_COLUMNS = 3012;
    public static final int PARSER_HIVE_NO_CLOSED_COLUMNS = 3013;
    public static final int PARSER_HIVE_NOT_SUPPORT_NON_OP_UNION = 3014;
    public static final int PARSER_HIVE_MISSING_FIELD_TYPE_INFO = 3015;
    public static final int PARSER_HIVE_NULL_FIELD = 3016;
    public static final int PARSER_HIVE_NULL_VALUE_IN_LIST = 3017;
    public static final int INPUT_RECORD_RECORD_WITH_METADATA_AND_PK_NULL_IN_NON_OPTIONAL = 3018;
    public static final int INPUT_RECORD_RECORD_WITH_METADATA_AND_PK_CANNT_GET_PKEY = 3019;
    public static final int FEED_CHANGE_FEED_CONNECTIVITY_ON_ALIVE_FEED = 3020;
    public static final int RECORD_READER_MALFORMED_INPUT_STREAM = 3021;
    public static final int PROVIDER_DATAFLOW_CONTROLLER_UNKNOWN_DATA_SOURCE = 3022;
    public static final int PROVIDER_DATASOURCE_FACTORY_UNKNOWN_INPUT_STREAM_FACTORY = 3023;
    public static final int UTIL_EXTERNAL_DATA_UTILS_FAIL_CREATE_STREAM_FACTORY = 3024;
    public static final int UNKNOWN_RECORD_READER_FACTORY = 3025;
    public static final int PROVIDER_STREAM_RECORD_READER_UNKNOWN_FORMAT = 3026;
    public static final int UNKNOWN_RECORD_FORMAT_FOR_META_PARSER = 3027;
    public static final int LIBRARY_JAVA_JOBJECTS_FIELD_ALREADY_DEFINED = 3028;
    public static final int LIBRARY_JAVA_JOBJECTS_UNKNOWN_FIELD = 3029;
    public static final int NODE_RESOLVER_NO_NODE_CONTROLLERS = 3031;
    public static final int NODE_RESOLVER_UNABLE_RESOLVE_HOST = 3032;
    public static final int INPUT_RECORD_CONVERTER_DCP_MSG_TO_RECORD_CONVERTER_UNKNOWN_DCP_REQUEST = 3033;
    public static final int FEED_DATAFLOW_FRAME_DISTR_REGISTER_FAILED_DATA_PROVIDER = 3034;
    public static final int INPUT_RECORD_READER_CHAR_ARRAY_RECORD_TOO_LARGE = 3038;
    public static final int LIBRARY_JOBJECT_ACCESSOR_CANNOT_PARSE_TYPE = 3039;
    public static final int LIBRARY_JOBJECT_UTIL_ILLEGAL_ARGU_TYPE = 3040;
    public static final int LIBRARY_EXTERNAL_FUNCTION_UNABLE_TO_LOAD_CLASS = 3041;
    public static final int LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_KIND = 3042;
    public static final int LIBRARY_EXTERNAL_FUNCTION_UNKNOWN_KIND = 3043;
    public static final int LIBRARY_EXTERNAL_LIBRARY_CLASS_REGISTERED = 3044;
    public static final int LIBRARY_JAVA_FUNCTION_HELPER_CANNOT_HANDLE_ARGU_TYPE = 3045;
    public static final int LIBRARY_JAVA_FUNCTION_HELPER_OBJ_TYPE_NOT_SUPPORTED = 3046;
    public static final int LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_NAME = 3047;
    public static final int OPERATORS_FEED_META_OPERATOR_DESCRIPTOR_INVALID_RUNTIME = 3048;
    public static final int PARSER_FACTORY_DELIMITED_DATA_PARSER_FACTORY_NOT_VALID_DELIMITER = 3049;
    public static final int PARSER_FACTORY_DELIMITED_DATA_PARSER_FACTORY_NOT_VALID_QUOTE = 3050;
    public static final int PARSER_FACTORY_DELIMITED_DATA_PARSER_FACTORY_QUOTE_DELIMITER_MISMATCH = 3051;
    public static final int INDEXING_EXTERNAL_FILE_INDEX_ACCESSOR_UNABLE_TO_FIND_FILE_INDEX = 3052;
    public static final int PARSER_ADM_DATA_PARSER_FIELD_NOT_NULL = 3053;
    public static final int PARSER_ADM_DATA_PARSER_TYPE_MISMATCH = 3054;
    public static final int PARSER_ADM_DATA_PARSER_UNEXPECTED_TOKEN_KIND = 3055;
    public static final int PARSER_ADM_DATA_PARSER_ILLEGAL_ESCAPE = 3056;
    public static final int PARSER_ADM_DATA_PARSER_RECORD_END_UNEXPECTED = 3057;
    public static final int PARSER_ADM_DATA_PARSER_EXTRA_FIELD_IN_CLOSED_RECORD = 3058;
    public static final int PARSER_ADM_DATA_PARSER_UNEXPECTED_TOKEN_WHEN_EXPECT_COMMA = 3059;
    public static final int PARSER_ADM_DATA_PARSER_FOUND_COMMA_WHEN = 3060;
    public static final int PARSER_ADM_DATA_PARSER_UNSUPPORTED_INTERVAL_TYPE = 3061;
    public static final int PARSER_ADM_DATA_PARSER_INTERVAL_NOT_CLOSED = 3062;
    public static final int PARSER_ADM_DATA_PARSER_INTERVAL_BEGIN_END_POINT_MISMATCH = 3063;
    public static final int PARSER_ADM_DATA_PARSER_INTERVAL_MISSING_COMMA = 3064;
    public static final int PARSER_ADM_DATA_PARSER_INTERVAL_INVALID_DATETIME = 3065;
    public static final int PARSER_ADM_DATA_PARSER_INTERVAL_UNSUPPORTED_TYPE = 3066;
    public static final int PARSER_ADM_DATA_PARSER_INTERVAL_INTERVAL_ARGUMENT_ERROR = 3067;
    public static final int PARSER_ADM_DATA_PARSER_LIST_FOUND_END_COLLECTION = 3068;
    public static final int PARSER_ADM_DATA_PARSER_LIST_FOUND_COMMA_BEFORE_LIST = 3069;
    public static final int PARSER_ADM_DATA_PARSER_LIST_FOUND_COMMA_EXPECTING_ITEM = 3070;
    public static final int PARSER_ADM_DATA_PARSER_LIST_FOUND_END_RECOD = 3071;
    public static final int PARSER_ADM_DATA_PARSER_CAST_ERROR = 3072;
    public static final int PARSER_ADM_DATA_PARSER_CONSTRUCTOR_MISSING_DESERIALIZER = 3073;
    public static final int PARSER_ADM_DATA_PARSER_WRONG_INSTANCE = 3074;
    public static final int PARSER_TWEET_PARSER_CLOSED_FIELD_NULL = 3075;
    public static final int UTIL_FILE_SYSTEM_WATCHER_NO_FILES_FOUND = 3076;
    public static final int UTIL_LOCAL_FILE_SYSTEM_UTILS_PATH_NOT_FOUND = 3077;
    public static final int UTIL_HDFS_UTILS_CANNOT_OBTAIN_HDFS_SCHEDULER = 3078;
    public static final int ACTIVE_MANAGER_SHUTDOWN = 3079;
    public static final int FEED_METADATA_UTIL_UNEXPECTED_FEED_DATATYPE = 3080;
    public static final int FEED_METADATA_SOCKET_ADAPTOR_SOCKET_NOT_PROPERLY_CONFIGURED = 3081;
    public static final int FEED_METADATA_SOCKET_ADAPTOR_SOCKET_INVALID_HOST_NC = 3082;
    public static final int PROVIDER_DATASOURCE_FACTORY_DUPLICATE_FORMAT_MAPPING = 3083;
    public static final int CANNOT_SUBSCRIBE_TO_FAILED_ACTIVE_ENTITY = 3084;
    public static final int FEED_UNKNOWN_ADAPTER_NAME = 3085;
    public static final int PROVIDER_STREAM_RECORD_READER_WRONG_CONFIGURATION = 3086;
    public static final int FEED_CONNECT_FEED_APPLIED_INVALID_FUNCTION = 3087;
    public static final int ACTIVE_MANAGER_INVALID_RUNTIME = 3088;
    public static final int ACTIVE_ENTITY_ALREADY_STARTED = 3089;
    public static final int ACTIVE_ENTITY_CANNOT_BE_STOPPED = 3090;
    public static final int CANNOT_ADD_DATASET_TO_ACTIVE_ENTITY = 3091;
    public static final int CANNOT_REMOVE_DATASET_FROM_ACTIVE_ENTITY = 3092;
    public static final int ACTIVE_ENTITY_IS_ALREADY_REGISTERED = 3093;
    public static final int CANNOT_ADD_INDEX_TO_DATASET_CONNECTED_TO_ACTIVE_ENTITY = 3094;
    public static final int CANNOT_REMOVE_INDEX_FROM_DATASET_CONNECTED_TO_ACTIVE_ENTITY = 3095;
    public static final int ACTIVE_NOTIFICATION_HANDLER_IS_SUSPENDED = 3096;
    public static final int ACTIVE_ENTITY_LISTENER_IS_NOT_REGISTERED = 3097;
    public static final int CANNOT_DERIGESTER_ACTIVE_ENTITY_LISTENER = 3098;
    public static final int DOUBLE_INITIALIZATION_OF_ACTIVE_NOTIFICATION_HANDLER = 3099;
    public static final int DOUBLE_RECOVERY_ATTEMPTS = 3101;
    public static final int UNREPORTED_TASK_FAILURE_EXCEPTION = 3102;
    public static final int ACTIVE_ENTITY_ALREADY_SUSPENDED = 3103;
    public static final int ACTIVE_ENTITY_CANNOT_RESUME_FROM_STATE = 3104;
    public static final int ACTIVE_RUNTIME_IS_ALREADY_REGISTERED = 3105;
    public static final int ACTIVE_RUNTIME_IS_NOT_REGISTERED = 3106;
    public static final int ACTIVE_EVENT_HANDLER_ALREADY_SUSPENDED = 3107;
    public static final int METADATA_DROP_FUCTION_IN_USE = 3109;
    public static final int FEED_FAILED_WHILE_GETTING_A_NEW_RECORD = 3110;
    public static final int FEED_START_FEED_WITHOUT_CONNECTION = 3111;
    public static final int PARSER_COLLECTION_ITEM_CANNOT_BE_NULL = 3112;
    public static final int FAILED_TO_PARSE_RECORD = 3113;
    public static final int FAILED_TO_PARSE_RECORD_CONTENT = 3114;
    public static final int FAILED_TO_PARSE_METADATA = 3115;
    public static final int INPUT_DECODE_FAILURE = 3116;
    public static final int FAILED_TO_PARSE_MALFORMED_LOG_RECORD = 3117;

    // Lifecycle management errors
    public static final int DUPLICATE_PARTITION_ID = 4000;

    // Extension errors
    public static final int EXTENSION_ID_CONFLICT = 4001;
    public static final int EXTENSION_COMPONENT_CONFLICT = 4002;
    public static final int UNSUPPORTED_MESSAGE_TYPE = 4003;
    public static final int INVALID_CONFIGURATION = 4004;
    public static final int UNSUPPORTED_REPLICATION_STRATEGY = 4005;

    // Lifecycle management errors pt.2
    public static final int CLUSTER_STATE_UNUSABLE = 4006;

    private ErrorCode() {
    }

    private static class Holder {
        private static final Map<Integer, String> errorMessageMap;

        static {
            // Loads the map that maps error codes to error message templates.
            try (InputStream resourceStream = ErrorCode.class.getClassLoader().getResourceAsStream(RESOURCE_PATH)) {
                errorMessageMap = ErrorMessageUtil.loadErrorMap(resourceStream);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        private Holder() {
        }
    }

    public static String getErrorMessage(int errorCode) {
        String msg = Holder.errorMessageMap.get(errorCode);
        if (msg == null) {
            throw new IllegalStateException("Undefined error code: " + errorCode);
        }
        return msg;
    }
}
