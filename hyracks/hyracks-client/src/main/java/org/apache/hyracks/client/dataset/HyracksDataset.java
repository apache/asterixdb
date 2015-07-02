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

import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.dataset.IHyracksDatasetDirectoryServiceConnection;
import org.apache.hyracks.api.dataset.IHyracksDatasetReader;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.client.net.ClientNetworkManager;

public class HyracksDataset implements IHyracksDataset {
    private final IHyracksDatasetDirectoryServiceConnection datasetDirectoryServiceConnection;

    private final ClientNetworkManager netManager;

    private final DatasetClientContext datasetClientCtx;

    public HyracksDataset(IHyracksClientConnection hcc, int frameSize, int nReaders) throws Exception {
        NetworkAddress ddsAddress = hcc.getDatasetDirectoryServiceInfo();
        datasetDirectoryServiceConnection = new HyracksDatasetDirectoryServiceConnection
            (ddsAddress.getAddress(), ddsAddress.getPort());

        netManager = new ClientNetworkManager(nReaders);
        netManager.start();

        datasetClientCtx = new DatasetClientContext(frameSize);
    }

    @Override
    public IHyracksDatasetReader createReader(JobId jobId, ResultSetId resultSetId) throws HyracksDataException {
        IHyracksDatasetReader reader = null;
        try {
            reader = new HyracksDatasetReader(datasetDirectoryServiceConnection, netManager, datasetClientCtx, jobId,
                    resultSetId);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        return reader;
    }
}
