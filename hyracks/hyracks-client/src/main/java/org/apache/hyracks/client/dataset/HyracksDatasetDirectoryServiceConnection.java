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
package edu.uci.ics.hyracks.client.dataset;

import java.net.InetSocketAddress;

import edu.uci.ics.hyracks.api.dataset.DatasetDirectoryRecord;
import edu.uci.ics.hyracks.api.dataset.DatasetJobRecord.Status;
import edu.uci.ics.hyracks.api.dataset.IHyracksDatasetDirectoryServiceConnection;
import edu.uci.ics.hyracks.api.dataset.IHyracksDatasetDirectoryServiceInterface;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.RPCInterface;
import edu.uci.ics.hyracks.ipc.impl.IPCSystem;
import edu.uci.ics.hyracks.ipc.impl.JavaSerializationBasedPayloadSerializerDeserializer;

//TODO(madhusudancs): Should this implementation be moved to edu.uci.ics.hyracks.client?
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
