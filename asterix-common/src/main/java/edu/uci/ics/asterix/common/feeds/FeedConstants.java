/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.feeds;

public class FeedConstants {

    public static final class StatisticsConstants {
        public static final String INTAKE_TUPLEID = "intake-tupleid";
        public static final String INTAKE_PARTITION = "intake-partition";
        public static final String INTAKE_TIMESTAMP = "intake-timestamp";
        public static final String COMPUTE_TIMESTAMP = "compute-timestamp";
        public static final String STORE_TIMESTAMP = "store-timestamp";

    }

    public static final class MessageConstants {
        public static final String MESSAGE_TYPE = "message-type";
        public static final String NODE_ID = "nodeId";
        public static final String DATAVERSE = "dataverse";
        public static final String FEED = "feed";
        public static final String DATASET = "dataset";
        public static final String AQL = "aql";
        public static final String RUNTIME_TYPE = "runtime-type";
        public static final String PARTITION = "partition";
        public static final String INTAKE_PARTITION = "intake-partition";
        public static final String INFLOW_RATE = "inflow-rate";
        public static final String OUTFLOW_RATE = "outflow-rate";
        public static final String MODE = "mode";
        public static final String CURRENT_CARDINALITY = "current-cardinality";
        public static final String REDUCED_CARDINALITY = "reduced-cardinality";
        public static final String VALUE_TYPE = "value-type";
        public static final String VALUE = "value";
        public static final String CPU_LOAD = "cpu-load";
        public static final String N_RUNTIMES = "n_runtimes";
        public static final String HEAP_USAGE = "heap_usage";
        public static final String OPERAND_ID = "operand-id";
        public static final String COMPUTE_PARTITION_RETAIN_LIMIT = "compute-partition-retain-limit";
        public static final String LAST_PERSISTED_TUPLE_INTAKE_TIMESTAMP = "last-persisted-tuple-intake_timestamp";
        public static final String PERSISTENCE_DELAY_WITHIN_LIMIT = "persistence-delay-within-limit";
        public static final String AVERAGE_PERSISTENCE_DELAY = "average-persistence-delay";
        public static final String COMMIT_ACKS = "commit-acks";
        public static final String MAX_WINDOW_ACKED = "max-window-acked";
        public static final String BASE = "base";
        public static final String NOT_APPLICABLE = "N/A";
        
    }

    public static final class NamingConstants {
        public static final String LIBRARY_NAME_SEPARATOR = "#";
    }

    public static class JobConstants {
        public static final int DEFAULT_FRAME_SIZE = 8192;
    }
}
