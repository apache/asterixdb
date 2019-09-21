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
package org.apache.hyracks.api.exceptions;

import java.io.InputStream;
import java.util.Map;

import org.apache.hyracks.api.util.ErrorMessageUtil;

/**
 * A registry of runtime/compile error codes
 * Error code:
 * 0 --- 9999: runtime errors
 * 10000 ---- 19999: compilation errors
 */
public class ErrorCode {
    private static final String RESOURCE_PATH = "errormsg/en.properties";
    public static final String HYRACKS = "HYR";

    // Runtime error codes.
    public static final int INVALID_OPERATOR_OPERATION = 1;
    public static final int ERROR_PROCESSING_TUPLE = 2;
    public static final int FAILURE_ON_NODE = 3;
    public static final int FILE_WITH_ABSOULTE_PATH_NOT_WITHIN_ANY_IO_DEVICE = 4;
    public static final int FULLTEXT_PHRASE_FOUND = 5;
    public static final int JOB_QUEUE_FULL = 6;
    public static final int INVALID_NETWORK_ADDRESS = 7;
    public static final int INVALID_INPUT_PARAMETER = 8;
    public static final int JOB_REQUIREMENTS_EXCEED_CAPACITY = 9;
    public static final int NO_SUCH_NODE = 10;
    public static final int CLASS_LOADING_ISSUE = 11;
    public static final int ILLEGAL_WRITE_AFTER_FLUSH_ATTEMPT = 12;
    public static final int DUPLICATE_IODEVICE = 13;
    public static final int NESTED_IODEVICES = 14;
    public static final int MORE_THAN_ONE_RESULT = 15;
    public static final int RESULT_FAILURE_EXCEPTION = 16;
    public static final int RESULT_FAILURE_NO_EXCEPTION = 17;
    public static final int INCONSISTENT_RESULT_METADATA = 18;
    public static final int CANNOT_DELETE_FILE = 19;
    public static final int NOT_A_JOBID = 20;
    public static final int ERROR_FINDING_DEPLOYED_JOB = 21;
    public static final int DUPLICATE_DEPLOYED_JOB = 22;
    public static final int DEPLOYED_JOB_FAILURE = 23;
    public static final int NO_RESULT_SET = 24;
    public static final int JOB_CANCELED = 25;
    public static final int NODE_FAILED = 26;
    public static final int FILE_IS_NOT_DIRECTORY = 27;
    public static final int CANNOT_READ_FILE = 28;
    public static final int UNIDENTIFIED_IO_ERROR_READING_FILE = 29;
    public static final int FILE_DOES_NOT_EXIST = 30;
    public static final int UNIDENTIFIED_IO_ERROR_DELETING_DIR = 31;
    public static final int RESULT_NO_RECORD = 32;
    public static final int DUPLICATE_KEY = 33;
    public static final int LOAD_NON_EMPTY_INDEX = 34;
    public static final int MODIFY_NOT_SUPPORTED_IN_EXTERNAL_INDEX = 35;
    public static final int FLUSH_NOT_SUPPORTED_IN_EXTERNAL_INDEX = 36;
    public static final int UPDATE_OR_DELETE_NON_EXISTENT_KEY = 37;
    public static final int INDEX_NOT_UPDATABLE = 38;
    public static final int OCCURRENCE_THRESHOLD_PANIC_EXCEPTION = 39;
    public static final int UNKNOWN_INVERTED_INDEX_TYPE = 40;
    public static final int CANNOT_PROPOSE_LINEARIZER_DIFF_DIMENSIONS = 41;
    public static final int CANNOT_PROPOSE_LINEARIZER_FOR_TYPE = 42;
    public static final int RECORD_IS_TOO_LARGE = 43;
    public static final int FAILED_TO_RE_FIND_PARENT = 44;
    public static final int FAILED_TO_FIND_TUPLE = 45;
    public static final int UNSORTED_LOAD_INPUT = 46;
    public static final int OPERATION_EXCEEDED_MAX_RESTARTS = 47;
    public static final int DUPLICATE_LOAD_INPUT = 48;
    public static final int CANNOT_CREATE_ACTIVE_INDEX = 49;
    public static final int CANNOT_ACTIVATE_ACTIVE_INDEX = 50;
    public static final int CANNOT_DEACTIVATE_INACTIVE_INDEX = 51;
    public static final int CANNOT_DESTROY_ACTIVE_INDEX = 52;
    public static final int CANNOT_CLEAR_INACTIVE_INDEX = 53;
    public static final int CANNOT_ALLOCATE_MEMORY_FOR_INACTIVE_INDEX = 54;
    public static final int RESOURCE_DOES_NOT_EXIST = 55;
    public static final int DISK_COMPONENT_SCAN_NOT_ALLOWED_FOR_SECONDARY_INDEX = 56;
    public static final int CANNOT_FIND_MATTER_TUPLE_FOR_ANTI_MATTER_TUPLE = 57;
    public static final int TASK_ABORTED = 58;
    public static final int OPEN_ON_OPEN_WRITER = 59;
    public static final int OPEN_ON_FAILED_WRITER = 60;
    public static final int NEXT_FRAME_ON_FAILED_WRITER = 61;
    public static final int NEXT_FRAME_ON_CLOSED_WRITER = 62;
    public static final int FLUSH_ON_FAILED_WRITER = 63;
    public static final int FLUSH_ON_CLOSED_WRITER = 64;
    public static final int FAIL_ON_FAILED_WRITER = 65;
    public static final int MISSED_FAIL_CALL = 66;
    public static final int CANNOT_CREATE_FILE = 67;
    public static final int NO_MAPPING_FOR_FILE_ID = 68;
    public static final int NO_MAPPING_FOR_FILENAME = 69;
    public static final int CANNOT_GET_NUMBER_OF_ELEMENT_FROM_INACTIVE_FILTER = 70;
    public static final int CANNOT_CREATE_BLOOM_FILTER_BUILDER_FOR_INACTIVE_FILTER = 71;
    public static final int CANNOT_CREATE_BLOOM_FILTER_WITH_NUMBER_OF_PAGES = 72;
    public static final int CANNOT_ADD_TUPLES_TO_DUMMY_BLOOM_FILTER = 73;
    public static final int CANNOT_CREATE_ACTIVE_BLOOM_FILTER = 74;
    public static final int CANNOT_DEACTIVATE_INACTIVE_BLOOM_FILTER = 75;
    public static final int CANNOT_DESTROY_ACTIVE_BLOOM_FILTER = 76;
    public static final int CANNOT_PURGE_ACTIVE_INDEX = 77;
    public static final int CANNOT_PURGE_ACTIVE_BLOOM_FILTER = 78;
    public static final int CANNOT_BULK_LOAD_NON_EMPTY_TREE = 79;
    public static final int CANNOT_CREATE_EXISTING_INDEX = 80;
    public static final int FILE_ALREADY_MAPPED = 81;
    public static final int FILE_ALREADY_EXISTS = 82;
    public static final int NO_INDEX_FOUND_WITH_RESOURCE_ID = 83;
    public static final int FOUND_OVERLAPPING_LSM_FILES = 84;
    public static final int FOUND_MULTIPLE_TRANSACTIONS = 85;
    public static final int UNRECOGNIZED_INDEX_COMPONENT_FILE = 86;
    public static final int UNEQUAL_NUM_FILTERS_TREES = 87;
    public static final int INDEX_NOT_MODIFIABLE = 88;
    public static final int GROUP_BY_MEMORY_BUDGET_EXCEEDS = 89;
    public static final int ILLEGAL_MEMORY_BUDGET = 90;
    public static final int TIMEOUT = 91;
    public static final int JOB_HAS_BEEN_CLEARED_FROM_HISTORY = 92;
    public static final int FAILED_TO_READ_RESULT = 93;
    public static final int CANNOT_READ_CLOSED_FILE = 94;
    public static final int TUPLE_CANNOT_FIT_INTO_EMPTY_FRAME = 95;
    public static final int ILLEGAL_ATTEMPT_TO_ENTER_EMPTY_COMPONENT = 96;
    public static final int ILLEGAL_ATTEMPT_TO_EXIT_EMPTY_COMPONENT = 97;
    public static final int A_FLUSH_OPERATION_HAS_FAILED = 98;
    public static final int A_MERGE_OPERATION_HAS_FAILED = 99;
    public static final int FAILED_TO_SHUTDOWN_EVENT_PROCESSOR = 100;
    public static final int PAGE_DOES_NOT_EXIST_IN_FILE = 101;
    public static final int VBC_ALREADY_OPEN = 102;
    public static final int VBC_ALREADY_CLOSED = 103;
    public static final int INDEX_DOES_NOT_EXIST = 104;
    public static final int CANNOT_DROP_IN_USE_INDEX = 105;
    public static final int CANNOT_DEACTIVATE_PINNED_BLOOM_FILTER = 106;
    public static final int PREDICATE_CANNOT_BE_NULL = 107;
    public static final int FULLTEXT_ONLY_EXECUTABLE_FOR_STRING_OR_LIST = 108;
    public static final int NOT_ENOUGH_BUDGET_FOR_TEXTSEARCH = 109;
    public static final int CANNOT_CONTINUE_TEXT_SEARCH_HYRACKS_TASK_IS_NULL = 110;
    public static final int CANNOT_CONTINUE_TEXT_SEARCH_BUFFER_MANAGER_IS_NULL = 111;
    public static final int CANNOT_ADD_ELEMENT_TO_INVERTED_INDEX_SEARCH_RESULT = 112;
    public static final int UNDEFINED_INVERTED_LIST_MERGE_TYPE = 113;
    public static final int NODE_IS_NOT_ACTIVE = 114;
    public static final int LOCAL_NETWORK_ERROR = 115;
    public static final int ONE_TUPLE_RANGEMAP_EXPECTED = 116;
    public static final int NO_RANGEMAP_PRODUCED = 117;
    public static final int RANGEMAP_NOT_FOUND = 118;
    public static final int UNSUPPORTED_WINDOW_SPEC = 119;
    public static final int EOF = 120;
    public static final int NUMERIC_PROMOTION_ERROR = 121;
    public static final int ERROR_PRINTING_PLAN = 122;

    // Compilation error codes.
    public static final int RULECOLLECTION_NOT_INSTANCE_OF_LIST = 10000;
    public static final int CANNOT_COMPOSE_PART_CONSTRAINTS = 10001;
    public static final int PHYS_OPERATOR_NOT_SET = 10002;
    public static final int DESCRIPTOR_GENERATION_ERROR = 10003;
    public static final int EXPR_NOT_NORMALIZED = 10004;
    public static final int OPERATOR_NOT_IMPLEMENTED = 10005;
    public static final int INAPPLICABLE_HINT = 10006;
    public static final int CROSS_PRODUCT_JOIN = 10007;
    public static final int GROUP_ALL_DECOR = 10008;

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

    private ErrorCode() {
    }

    public static String getErrorMessage(int errorCode) {
        String msg = Holder.errorMessageMap.get(errorCode);
        if (msg == null) {
            throw new IllegalStateException("Undefined error code: " + errorCode);
        }
        return msg;
    }
}
