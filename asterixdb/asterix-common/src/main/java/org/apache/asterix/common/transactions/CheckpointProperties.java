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
package org.apache.asterix.common.transactions;

import org.apache.asterix.common.config.TransactionProperties;

public class CheckpointProperties {

    private final String checkpointDirPath;
    private final int lsnThreshold;
    private final int pollFrequency;
    private final int historyToKeep;
    private final int datasetCheckpointInterval;

    public CheckpointProperties(TransactionProperties txnProperties, String nodeId) {
        // Currently we use the log files directory for checkpoints
        checkpointDirPath = txnProperties.getLogDirectory(nodeId);
        lsnThreshold = txnProperties.getCheckpointLSNThreshold();
        pollFrequency = txnProperties.getCheckpointPollFrequency();
        historyToKeep = txnProperties.getCheckpointHistory();
        datasetCheckpointInterval = txnProperties.getDatasetCheckpointInterval();
    }

    public int getLsnThreshold() {
        return lsnThreshold;
    }

    public int getPollFrequency() {
        return pollFrequency;
    }

    public int getHistoryToKeep() {
        return historyToKeep;
    }

    public String getCheckpointDirPath() {
        return checkpointDirPath;
    }

    public int getDatasetCheckpointInterval() {
        return datasetCheckpointInterval;
    }

    @Override
    public String toString() {
        return "{\"class\" : \"" + getClass().getSimpleName() + "\", \"checkpoint-dir-path\" : \"" + checkpointDirPath
                + "\", \"lsn-threshold\" : " + lsnThreshold + ", \"poll-frequency\" : " + pollFrequency
                + ", \"history-to-keep\" : " + historyToKeep + ", \"dataset-checkpoint-interval\" : "
                + datasetCheckpointInterval + "}";
    }
}
