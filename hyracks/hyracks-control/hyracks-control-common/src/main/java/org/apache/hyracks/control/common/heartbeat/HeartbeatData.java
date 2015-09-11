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
package org.apache.hyracks.control.common.heartbeat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HeartbeatData {

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
    public long diskReads;
    public long diskWrites;

    public void readFields(DataInput dis) throws IOException {
        heapInitSize = dis.readLong();
        heapUsedSize = dis.readLong();
        heapCommittedSize = dis.readLong();
        heapMaxSize = dis.readLong();
        nonheapInitSize = dis.readLong();
        nonheapUsedSize = dis.readLong();
        nonheapCommittedSize = dis.readLong();
        nonheapMaxSize = dis.readLong();
        threadCount = dis.readInt();
        peakThreadCount = dis.readInt();
        totalStartedThreadCount = dis.readLong();
        systemLoadAverage = dis.readDouble();
        netPayloadBytesRead = dis.readLong();
        netPayloadBytesWritten = dis.readLong();
        netSignalingBytesRead = dis.readLong();
        netSignalingBytesWritten = dis.readLong();
        netSignalingBytesWritten = dis.readLong();
        datasetNetPayloadBytesWritten = dis.readLong();
        datasetNetSignalingBytesRead = dis.readLong();
        datasetNetSignalingBytesWritten = dis.readLong();
        ipcMessagesSent = dis.readLong();
        ipcMessageBytesSent = dis.readLong();
        ipcMessagesReceived = dis.readLong();
        ipcMessageBytesReceived = dis.readLong();
        diskReads = dis.readLong();
        diskWrites = dis.readLong();

        int gcCounts = dis.readInt();
        gcCollectionCounts = new long[gcCounts];
        for (int i = 0; i < gcCollectionCounts.length; i++) {
            gcCollectionCounts[i] = dis.readLong();
        }
        int gcTimeCounts = dis.readInt();
        gcCollectionTimes = new long[gcTimeCounts];
        for (int i = 0; i < gcCollectionTimes.length; i++) {
            gcCollectionTimes[i] = dis.readLong();
        }
    }

    public void write(DataOutput dos) throws IOException {
        dos.writeLong(heapInitSize);
        dos.writeLong(heapUsedSize);
        dos.writeLong(heapCommittedSize);
        dos.writeLong(heapMaxSize);
        dos.writeLong(nonheapInitSize);
        dos.writeLong(nonheapUsedSize);
        dos.writeLong(nonheapCommittedSize);
        dos.writeLong(nonheapMaxSize);
        dos.writeInt(threadCount);
        dos.writeInt(peakThreadCount);
        dos.writeLong(totalStartedThreadCount);
        dos.writeDouble(systemLoadAverage);
        dos.writeLong(netPayloadBytesRead);
        dos.writeLong(netPayloadBytesWritten);
        dos.writeLong(netSignalingBytesRead);
        dos.writeLong(netSignalingBytesWritten);
        dos.writeLong(datasetNetPayloadBytesRead);
        dos.writeLong(datasetNetPayloadBytesWritten);
        dos.writeLong(datasetNetSignalingBytesRead);
        dos.writeLong(datasetNetSignalingBytesWritten);
        dos.writeLong(ipcMessagesSent);
        dos.writeLong(ipcMessageBytesSent);
        dos.writeLong(ipcMessagesReceived);
        dos.writeLong(ipcMessageBytesReceived);
        dos.writeLong(diskReads);
        dos.writeLong(diskWrites);

        dos.writeInt(gcCollectionCounts.length);
        for (int i = 0; i < gcCollectionCounts.length; i++) {
            dos.writeLong(gcCollectionCounts[i]);
        }
        dos.writeInt(gcCollectionTimes.length);
        for (int i = 0; i < gcCollectionTimes.length; i++) {
            dos.writeLong(gcCollectionTimes[i]);
        }
    }

}