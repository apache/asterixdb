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
package org.apache.asterix.common.config;

public class DatasetConfig {

    /*
     * We have two kinds of datasets. INTERNAL: A dataset with data persisted
     * in ASTERIX storage. The dataset is populated either using a load
     * statement or using insert statement. EXTERNAL: A dataset with data
     * residing outside ASTERIX. As such ASTERIX does not maintain any indexes
     * on the data. The data for the dataset is fetched as and when required
     * from an external data source using an adapter.
     */
    public enum DatasetType {
        INTERNAL,
        EXTERNAL
    }

    public enum IndexType {
        BTREE,
        RTREE,
        SINGLE_PARTITION_WORD_INVIX,
        SINGLE_PARTITION_NGRAM_INVIX,
        LENGTH_PARTITIONED_WORD_INVIX,
        LENGTH_PARTITIONED_NGRAM_INVIX
    }

    public enum ExternalDatasetTransactionState {
        COMMIT, // The committed state <- nothing is required->
        BEGIN, // The state after starting the refresh transaction <- will either abort moving to committed state or move to ready to commit->
        READY_TO_COMMIT // The transaction is ready to commit <- can only move forward to committed state-> 
    };

    public enum ExternalFilePendingOp {
        PENDING_NO_OP, // the stored file is part of a committed transaction nothing is required
        PENDING_ADD_OP, // the stored file is part of an ongoing transaction (will be added if transaction succeed)
        PENDING_DROP_OP, // the stored file is part of an ongoing transaction (will be dropped if transaction succeed)
        PENDING_APPEND_OP // the stored file is part of an ongoing transaction (will be updated if transaction succeed)
    };
}
