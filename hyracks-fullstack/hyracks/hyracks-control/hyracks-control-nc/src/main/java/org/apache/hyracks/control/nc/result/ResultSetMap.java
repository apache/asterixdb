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
package org.apache.hyracks.control.nc.result;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultStateRecord;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ResultSetMap implements IResultStateRecord, Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger();

    private final long timestamp;
    private final HashMap<ResultSetId, ResultState[]> resultStateMap;

    ResultSetMap() {
        timestamp = System.nanoTime();
        resultStateMap = new HashMap<>();
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    ResultState[] getResultStates(ResultSetId rsId) {
        return resultStateMap.get(rsId);
    }

    ResultState[] createOrGetResultStates(ResultSetId rsId, int nPartitions) {
        return resultStateMap.computeIfAbsent(rsId, (k) -> new ResultState[nPartitions]);
    }

    /**
     * removes a result partition for a result set
     *
     * @param jobId
     *            the id of the job that produced the result set
     * @param resultSetId
     *            the id of the result set
     * @param partition
     *            the partition number
     * @return true, if all partitions for the resultSetId have been removed
     */
    boolean removePartition(JobId jobId, ResultSetId resultSetId, int partition) {
        final ResultState[] resultStates = resultStateMap.get(resultSetId);
        if (resultStates != null) {
            final ResultState state = resultStates[partition];
            if (state != null) {
                state.closeAndDelete();
                LOGGER.trace("Removing partition: {} for JobId: {}", partition, jobId);
            }
            resultStates[partition] = null;
            boolean stateEmpty = true;
            for (ResultState resState : resultStates) {
                if (resState != null) {
                    stateEmpty = false;
                    break;
                }
            }
            if (stateEmpty) {
                resultStateMap.remove(resultSetId);
            }
            return resultStateMap.isEmpty();
        }
        return true;
    }

    void abortAll() {
        applyToAllStates((rsId, state, i) -> state.abort());
    }

    void closeAndDeleteAll() {
        applyToAllStates((rsId, state, i) -> {
            state.closeAndDelete();
            LOGGER.trace("Removing partition: {} for result set {}", i, rsId);
        });
    }

    @FunctionalInterface
    private interface StateModifier {
        void modify(ResultSetId rsId, ResultState entry, int partition);
    }

    private void applyToAllStates(StateModifier modifier) {
        for (Map.Entry<ResultSetId, ResultState[]> entry : resultStateMap.entrySet()) {
            final ResultSetId rsId = entry.getKey();
            final ResultState[] resultStates = entry.getValue();
            if (resultStates == null) {
                continue;
            }
            for (int i = 0; i < resultStates.length; i++) {
                final ResultState state = resultStates[i];
                if (state != null) {
                    modifier.modify(rsId, state, i);
                }
            }
        }
    }
}
