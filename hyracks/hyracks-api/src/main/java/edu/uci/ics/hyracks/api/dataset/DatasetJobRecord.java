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
package edu.uci.ics.hyracks.api.dataset;

import java.util.HashMap;
import java.util.List;

public class DatasetJobRecord extends HashMap<ResultSetId, ResultSetMetaData> implements IDatasetStateRecord {
    public enum Status {
        RUNNING,
        SUCCESS,
        FAILED
    }

    private static final long serialVersionUID = 1L;

    private final long timestamp;

    private Status status;

    private List<Exception> exceptions;

    public DatasetJobRecord() {
        this.timestamp = System.currentTimeMillis();
        this.status = Status.RUNNING;
    }

    public void start() {
        status = Status.RUNNING;
    }

    public void success() {
        status = Status.SUCCESS;
    }

    public void fail() {
        status = Status.FAILED;
    }

    public void fail(List<Exception> exceptions) {
        status = Status.FAILED;
        this.exceptions = exceptions;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Status getStatus() {
        return status;
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }
}
