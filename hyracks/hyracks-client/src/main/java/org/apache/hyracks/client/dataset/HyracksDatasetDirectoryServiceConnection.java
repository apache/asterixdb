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
package org.apache.hyracks.client.dataset;

import java.net.InetSocketAddress;

import org.apache.hyracks.api.dataset.DatasetDirectoryRecord;
import org.apache.hyracks.api.dataset.DatasetJobRecord.Status;
import org.apache.hyracks.api.dataset.IHyracksDatasetDirectoryServiceConnection;
import org.apache.hyracks.api.dataset.IHyracksDatasetDirectoryServiceInterface;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.RPCInterface;
import org.apache.hyracks.ipc.impl.IPCSystem;
import org.apache.hyracks.ipc.impl.JavaSerializationBasedPayloadSerializerDeserializer;

//TODO(madhusudancs): Should this implementation be moved to org.apache.hyracks.client?
public class HyracksDatasetDirectoryServiceConnection implements IHyracksDatasetDirectoryServiceConnection {
    private final IPCSystem ipc;
    private final IHyracksDatasetDirectoryServiceInterface ddsi;

    public HyracksDatasetDirectoryServiceConnection(String ddsHost, int ddsPort) throws Exception {
        RPCInterface rpci = new RPCInterface();
        ipc = new IPCSystem(new InetSocketAddress(0), rpci, new JavaSerializationBasedPayloadSerializerDeserializer());
        ipc.start();
        IIPCHandle ddsIpchandle = ipc.getHandle(new InetSocketAddress(ddsHost, ddsPort));
        this.ddsi = new HyracksDatasetDirectoryServiceInterfaceRemoteProxy(ddsIpchandle, rpci);
    }

    @Override
    public Status getDatasetResultStatus(JobId jobId, ResultSetId rsId) throws Exception {
        return ddsi.getDatasetResultStatus(jobId, rsId);
    }

    @Override
    public DatasetDirectoryRecord[] getDatasetResultLocations(JobId jobId, ResultSetId rsId,
            DatasetDirectoryRecord[] knownRecords) throws Exception {
        return ddsi.getDatasetResultLocations(jobId, rsId, knownRecords);
    }
}
