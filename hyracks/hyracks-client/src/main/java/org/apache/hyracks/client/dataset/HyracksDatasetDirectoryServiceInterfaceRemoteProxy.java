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
package org.apache.hyracks.client.dataset;

import org.apache.hyracks.api.client.HyracksClientInterfaceFunctions;
import org.apache.hyracks.api.dataset.DatasetDirectoryRecord;
import org.apache.hyracks.api.dataset.DatasetJobRecord.Status;
import org.apache.hyracks.api.dataset.IHyracksDatasetDirectoryServiceInterface;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.RPCInterface;

//TODO(madhusudancs): Should this implementation be moved to org.apache.hyracks.client?
public class HyracksDatasetDirectoryServiceInterfaceRemoteProxy implements IHyracksDatasetDirectoryServiceInterface {
    private final IIPCHandle ipcHandle;

    private final RPCInterface rpci;

    public HyracksDatasetDirectoryServiceInterfaceRemoteProxy(IIPCHandle ipcHandle, RPCInterface rpci) {
        this.ipcHandle = ipcHandle;
        this.rpci = rpci;
    }

    @Override
    public Status getDatasetResultStatus(JobId jobId, ResultSetId rsId) throws Exception {
        HyracksClientInterfaceFunctions.GetDatasetResultStatusFunction gdrlf = new HyracksClientInterfaceFunctions.GetDatasetResultStatusFunction(
                jobId, rsId);
        return (Status) rpci.call(ipcHandle, gdrlf);
    }

    @Override
    public DatasetDirectoryRecord[] getDatasetResultLocations(JobId jobId, ResultSetId rsId,
            DatasetDirectoryRecord[] knownRecords) throws Exception {
        HyracksClientInterfaceFunctions.GetDatasetResultLocationsFunction gdrlf = new HyracksClientInterfaceFunctions.GetDatasetResultLocationsFunction(
                jobId, rsId, knownRecords);
        return (DatasetDirectoryRecord[]) rpci.call(ipcHandle, gdrlf);
    }
}
