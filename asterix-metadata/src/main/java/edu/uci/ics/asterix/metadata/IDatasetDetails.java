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
package edu.uci.ics.asterix.metadata;

import java.io.DataOutput;
import java.io.Serializable;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IDatasetDetails extends Serializable {

    public DatasetType getDatasetType();

    public void writeDatasetDetailsRecordType(DataOutput out) throws HyracksDataException;

    /**
     * @return if the dataset is a temporary dataset.
     *         Here is a summary of temporary datasets:
     *         1. Different from a persistent dataset, reads and writes over a temporary dataset do not require any lock.
     *         Writes over a temporary dataset do not generate any write-ahead update and commit log but generate
     *         flush log and job commit log.
     *         2. A temporary dataset can only be an internal dataset, stored in partitioned LSM-Btrees.
     *         3. All secondary indexes for persistent datasets are supported for temporary datasets.
     *         4. A temporary dataset will be automatically garbage collected if it is not active in the past 30 days.
     *         A temporary dataset could be used for the following scenarios:
     *         1. A data scientist wants to run some one-time data analysis queries over a dataset that s/he pre-processed
     *         and the dataset is only used by her/himself in an one-query-at-a-time manner.
     *         2. Articulate AQL with external systems such as Pregelix/IMRU/Spark. A user can first run an AQL
     *         query to populate a temporary dataset, then kick off an external runtime to read this dataset,
     *         dump the results of the external runtime to yet-another-temporary dataset, and finally run yet-another AQL
     *         over the second temporary dataset.
     */
    public boolean isTemp();

    public long getLastAccessTime();
}
