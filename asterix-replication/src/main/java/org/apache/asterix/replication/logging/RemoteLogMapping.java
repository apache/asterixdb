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
package org.apache.asterix.replication.logging;

import java.util.concurrent.atomic.AtomicInteger;

public class RemoteLogMapping {

    private String remoteNodeID;
    private long remoteLSN;
    private boolean isFlushed = false;
    private long localLSN;
    public AtomicInteger numOfFlushedIndexes = new AtomicInteger();

    public boolean isFlushed() {
        return isFlushed;
    }

    public void setFlushed(boolean isFlushed) {
        this.isFlushed = isFlushed;
    }

    public String getRemoteNodeID() {
        return remoteNodeID;
    }

    public void setRemoteNodeID(String remoteNodeID) {
        this.remoteNodeID = remoteNodeID;
    }

    public long getRemoteLSN() {
        return remoteLSN;
    }

    public void setRemoteLSN(long remoteLSN) {
        this.remoteLSN = remoteLSN;
    }

    public long getLocalLSN() {
        return localLSN;
    }

    public void setLocalLSN(long localLSN) {
        this.localLSN = localLSN;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Remote Node: " + remoteNodeID);
        sb.append(" Remote LSN: " + remoteLSN);
        sb.append(" Local LSN: " + localLSN);
        sb.append(" isFlushed : " + isFlushed);
        return sb.toString();
    }
}