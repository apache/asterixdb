/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import edu.uci.ics.hyracks.api.client.HyracksClientInterfaceFunctions;
import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataset.IHyracksDatasetDirectoryServiceInterface;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.RPCInterface;

//TODO(madhusudancs): Should this implementation be moved to edu.uci.ics.hyracks.client?
public class HyracksDatasetDirectoryServiceInterfaceRemoteProxy implements IHyracksDatasetDirectoryServiceInterface {
    private final IIPCHandle ipcHandle;

    private final RPCInterface rpci;

    public HyracksDatasetDirectoryServiceInterfaceRemoteProxy(IIPCHandle ipcHandle, RPCInterface rpci) {
        this.ipcHandle = ipcHandle;
        this.rpci = rpci;
    }

    @Override
    public NetworkAddress[] getDatasetResultLocationsFunction(JobId jobId, NetworkAddress[] knownLocations)
            throws Exception {
        HyracksClientInterfaceFunctions.GetDatasetResultLocationsFunction gdrlf = new HyracksClientInterfaceFunctions.GetDatasetResultLocationsFunction(
                jobId, knownLocations);
        return (NetworkAddress[]) rpci.call(ipcHandle, gdrlf);
    }
}
