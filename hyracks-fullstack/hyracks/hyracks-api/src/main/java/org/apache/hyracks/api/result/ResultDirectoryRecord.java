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

import org.apache.hyracks.api.comm.NetworkAddress;

public class ResultDirectoryRecord implements Serializable {
    public enum Status {
        IDLE,
        RUNNING,
        SUCCESS,
        FAILED
    }

    private static final long serialVersionUID = 1L;

    private NetworkAddress address;

    private boolean readEOS;

    private Status status;

    private boolean empty;

    public ResultDirectoryRecord() {
        this.address = null;
        this.readEOS = false;
        this.status = Status.IDLE;
    }

    public void setNetworkAddress(NetworkAddress address) {
        this.address = address;
    }

    public NetworkAddress getNetworkAddress() {
        return address;
    }

    public void setEmpty(boolean empty) {
        this.empty = empty;
    }

    public boolean isEmpty() {
        return empty;
    }

    public void readEOS() {
        this.readEOS = true;
    }

    public boolean hasReachedReadEOS() {
        return readEOS;
    }

    public void start() {
        updateStatus(Status.RUNNING);
    }

    public void writeEOS() {
        updateStatus(Status.SUCCESS);
    }

    public void fail() {
        status = Status.FAILED;
    }

    private void updateStatus(final ResultDirectoryRecord.Status newStatus) {
        // FAILED is a stable status
        if (status != Status.FAILED) {
            status = newStatus;
        }
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof ResultDirectoryRecord)) {
            return false;
        }
        return address.equals(((ResultDirectoryRecord) o).address);
    }

    @Override
    public String toString() {
        return String.valueOf(address) + " " + status + (empty ? " (empty)" : "") + (readEOS ? " (EOS)" : "");
    }
}
