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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

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

    private final long timestamp;

    private Status status;

    private Map<ResultSetId, ResultSetMetaData> resultSetMetadataMap = new HashMap<>();

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
        updateState(State.RUNNING);
    }

    public void success() {
        updateState(State.SUCCESS);
    }

    public void fail(ResultSetId rsId, int partition) {
        getOrCreateDirectoryRecord(rsId, partition).fail();
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
        sb.append("\"resultsets\": ").append(Arrays.toString(resultSetMetadataMap.entrySet().toArray())).append(" }");
        return sb.toString();
    }

    public void setResultSetMetaData(ResultSetId rsId, boolean orderedResult, int nPartitions)
            throws HyracksDataException {
        ResultSetMetaData rsMd = resultSetMetadataMap.get(rsId);
        if (rsMd == null) {
            resultSetMetadataMap.put(rsId, new ResultSetMetaData(nPartitions, orderedResult));
        } else if (rsMd.getOrderedResult() != orderedResult || rsMd.getRecords().length != nPartitions) {
            throw HyracksDataException.create(ErrorCode.INCONSISTENT_RESULT_METADATA, rsId.toString());
        }
        //TODO(tillw) throwing a HyracksDataException here hangs the execution tests
    }

    public ResultSetMetaData getResultSetMetaData(ResultSetId rsId) {
        return resultSetMetadataMap.get(rsId);
    }

    public synchronized ResultDirectoryRecord getOrCreateDirectoryRecord(ResultSetId rsId, int partition) {
        ResultDirectoryRecord[] records = getResultSetMetaData(rsId).getRecords();
        if (records[partition] == null) {
            records[partition] = new ResultDirectoryRecord();
        }
        return records[partition];
    }

    public synchronized ResultDirectoryRecord getDirectoryRecord(ResultSetId rsId, int partition)
            throws HyracksDataException {
        ResultDirectoryRecord[] records = getResultSetMetaData(rsId).getRecords();
        if (records[partition] == null) {
            throw HyracksDataException.create(ErrorCode.RESULT_NO_RECORD, partition, rsId);
        }
        return records[partition];
    }

    public synchronized void updateState(ResultSetId rsId) {
        int successCount = 0;
        ResultDirectoryRecord[] records = getResultSetMetaData(rsId).getRecords();
        for (ResultDirectoryRecord record : records) {
            if ((record != null) && (record.getStatus() == ResultDirectoryRecord.Status.SUCCESS)) {
                successCount++;
            }
        }
        if (successCount == records.length) {
            success();
        }
    }
}
