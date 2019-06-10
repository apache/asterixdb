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
package org.apache.hyracks.api.result;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResultJobRecord implements IResultStateRecord {

    public enum State {
        IDLE,
        RUNNING,
        SUCCESS,
        FAILED
    }

    public static class Status implements Serializable {

        private static final long serialVersionUID = 1L;

        State state = State.IDLE;

        private List<Exception> exceptions;

        public State getState() {
            return state;
        }

        void setState(State state) {
            this.state = state;
        }

        public List<Exception> getExceptions() {
            return exceptions;
        }

        void setExceptions(List<Exception> exceptions) {
            this.exceptions = exceptions;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{ \"state\": \"").append(state.name()).append("\"");
            if (exceptions != null && !exceptions.isEmpty()) {
                sb.append(", \"exceptions\": ");
                List<String> msgs = new ArrayList<>();
                exceptions.forEach(e -> msgs.add("\"" + e.getMessage() + "\""));
                sb.append(Arrays.toString(msgs.toArray()));
            }
            sb.append(" }");
            return sb.toString();
        }
    }

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    private final long timestamp;
    private long jobStartTime;
    private long jobEndTime;
    private Status status;
    private ResultSetId rsId;
    private ResultSetMetaData resultSetMetaData;

    public ResultJobRecord() {
        this.timestamp = System.nanoTime();
        this.status = new Status();
    }

    private void updateState(State newStatus) {
        // FAILED is a stable status
        if (status.state != State.FAILED) {
            status.setState(newStatus);
        }
    }

    public void start() {
        jobStartTime = System.nanoTime();
        updateState(State.RUNNING);
    }

    public void finish() {
        jobEndTime = System.nanoTime();
    }

    public long getJobDuration() {
        return jobEndTime - jobStartTime;
    }

    public void success() {
        updateState(State.SUCCESS);
    }

    public void fail(List<Exception> exceptions) {
        updateState(State.FAILED);
        status.setExceptions(exceptions);
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ \"status\": ").append(status.toString()).append(", ");
        sb.append("\"timestamp\": ").append(timestamp).append(", ");
        sb.append("\"resultset\": ").append(resultSetMetaData).append(" }");
        return sb.toString();
    }

    public synchronized void setResultSetMetaData(ResultSetId rsId, IResultMetadata metadata, int nPartitions)
            throws HyracksDataException {
        if (this.rsId == null) {
            this.rsId = rsId;
            this.resultSetMetaData = new ResultSetMetaData(nPartitions, metadata);
        } else if (!this.rsId.equals(rsId) || resultSetMetaData.getRecords().length != nPartitions) {
            logInconsistentMetadata(rsId, nPartitions);
            throw HyracksDataException.create(ErrorCode.INCONSISTENT_RESULT_METADATA, this.rsId.toString());
        }
    }

    public synchronized ResultDirectoryRecord getOrCreateDirectoryRecord(int partition) {
        ResultDirectoryRecord[] records = resultSetMetaData.getRecords();
        if (records[partition] == null) {
            records[partition] = new ResultDirectoryRecord();
        }
        return records[partition];
    }

    public synchronized ResultDirectoryRecord getDirectoryRecord(int partition) throws HyracksDataException {
        ResultDirectoryRecord[] records = resultSetMetaData.getRecords();
        if (records[partition] == null) {
            throw HyracksDataException.create(ErrorCode.RESULT_NO_RECORD, partition, rsId);
        }
        return records[partition];
    }

    public synchronized void updateState() {
        int successCount = 0;
        ResultDirectoryRecord[] records = resultSetMetaData.getRecords();
        for (ResultDirectoryRecord record : records) {
            if ((record != null) && (record.getStatus() == ResultDirectoryRecord.Status.SUCCESS)) {
                successCount++;
            }
        }
        if (successCount == records.length) {
            success();
        }
    }

    public synchronized ResultSetMetaData getResultSetMetaData() {
        return resultSetMetaData;
    }

    private void logInconsistentMetadata(ResultSetId rsId, int nPartitions) {
        if (LOGGER.isWarnEnabled()) {
            LOGGER.warn("inconsistent result metadata for result set {}", this.rsId);
            if (!this.rsId.equals(rsId)) {
                LOGGER.warn("inconsistent result set id. Current {}, new {}", this.rsId, rsId);
            }
            final int expectedPartitions = resultSetMetaData.getRecords().length;
            if (expectedPartitions != nPartitions) {
                LOGGER.warn("inconsistent result set number of partitions. Current {}, new {}", expectedPartitions,
                        nPartitions);
            }
        }
    }
}
