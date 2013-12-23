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
package edu.uci.ics.asterix.common.config;

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

}
