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
package org.apache.hyracks.api.dataset;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class DatasetJobRecord implements IDatasetStateRecord {
    public enum Status {
        IDLE,
        RUNNING,
        SUCCESS,
        FAILED
    }

    private static final long serialVersionUID = 1L;

    private final long timestamp;

    private Status status;

    private List<Exception> exceptions;

    private Map<ResultSetId, ResultSetMetaData> resultSetMetadataMap = new HashMap<>();

    public DatasetJobRecord() {
        this.timestamp = System.currentTimeMillis();
        this.status = Status.IDLE;
    }

    private void updateStatus(Status newStatus) {
        // FAILED is a stable status
        if (status != Status.FAILED) {
            status = newStatus;
        }
    }

    public void start() {
        updateStatus(Status.RUNNING);
    }

    public void success() {
        updateStatus(Status.SUCCESS);
    }

    public void fail(ResultSetId rsId, int partition) {
        getOrCreateDirectoryRecord(rsId, partition).fail();
        status = Status.FAILED;
    }

    public void fail(List<Exception> exceptions) {
        status = Status.FAILED;
        this.exceptions = exceptions;
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
        return resultSetMetadataMap.toString();
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }

    public void setResultSetMetaData(ResultSetId rsId, boolean orderedResult, int nPartitions) throws
            HyracksDataException {
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

    public synchronized DatasetDirectoryRecord getOrCreateDirectoryRecord(ResultSetId rsId, int partition) {
        DatasetDirectoryRecord[] records = getResultSetMetaData(rsId).getRecords();
        if (records[partition] == null) {
            records[partition] = new DatasetDirectoryRecord();
        }
        return records[partition];
    }

    public synchronized DatasetDirectoryRecord getDirectoryRecord(ResultSetId rsId, int partition) throws
            HyracksDataException {
        DatasetDirectoryRecord[] records = getResultSetMetaData(rsId).getRecords();
        if (records[partition] == null) {
            throw new HyracksDataException("no record for partition " + partition + " of result set " + rsId);
        }
        return records[partition];
    }

    public synchronized void updateStatus(ResultSetId rsId) {
        int successCount = 0;
        DatasetDirectoryRecord[] records = getResultSetMetaData(rsId).getRecords();
        for (DatasetDirectoryRecord record : records) {
            if ((record != null) && (record.getStatus() == DatasetDirectoryRecord.Status.SUCCESS)) {
                successCount++;
            }
        }
        if (successCount == records.length) {
            success();
        }
    }
}
