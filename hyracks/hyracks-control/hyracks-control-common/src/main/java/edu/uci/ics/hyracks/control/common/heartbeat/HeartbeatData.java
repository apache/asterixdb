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
package edu.uci.ics.hyracks.control.common.heartbeat;

import java.io.Serializable;

public class HeartbeatData implements Serializable {
    private static final long serialVersionUID = 1L;

    public long heapInitSize;
    public long heapUsedSize;
    public long heapCommittedSize;
    public long heapMaxSize;
    public long nonheapInitSize;
    public long nonheapUsedSize;
    public long nonheapCommittedSize;
    public long nonheapMaxSize;
    public int threadCount;
    public int peakThreadCount;
    public long totalStartedThreadCount;
    public double systemLoadAverage;
    public long[] gcCollectionCounts;
    public long[] gcCollectionTimes;
    public long netPayloadBytesRead;
    public long netPayloadBytesWritten;
    public long netSignalingBytesRead;
    public long netSignalingBytesWritten;
    public long datasetNetPayloadBytesRead;
    public long datasetNetPayloadBytesWritten;
    public long datasetNetSignalingBytesRead;
    public long datasetNetSignalingBytesWritten;
    public long ipcMessagesSent;
    public long ipcMessageBytesSent;
    public long ipcMessagesReceived;
    public long ipcMessageBytesReceived;
}