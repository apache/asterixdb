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
package org.apache.hyracks.client.result;

import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.context.IHyracksCommonContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.network.ISocketChannelFactory;
import org.apache.hyracks.api.result.IResultDirectory;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.api.result.IResultSetReader;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.client.net.ClientNetworkManager;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;

public class ResultSet implements IResultSet {
    private final IResultDirectory resultDirectory;

    private final ClientNetworkManager netManager;

    private final IHyracksCommonContext resultClientCtx;

    public ResultSet(IHyracksClientConnection hcc, ISocketChannelFactory socketChannelFactory, int frameSize,
            int nReaders) throws Exception {
        NetworkAddress ddsAddress = hcc.getResultDirectoryAddress();
        resultDirectory = new ResultDirectory(ddsAddress.getAddress(), ddsAddress.getPort(), socketChannelFactory);

        netManager = new ClientNetworkManager(nReaders, socketChannelFactory);
        netManager.start();

        resultClientCtx = new ResultClientContext(frameSize);
    }

    @Override
    public IResultSetReader createReader(JobId jobId, ResultSetId resultSetId) throws HyracksDataException {
        IResultSetReader reader = null;
        try {
            reader = new ResultSetReader(resultDirectory, netManager, resultClientCtx, jobId, resultSetId);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
        return reader;
    }

    static class ResultClientContext extends FrameManager implements IHyracksCommonContext {

        ResultClientContext(int frameSize) {
            super(frameSize);
        }

        @Override
        public IIOManager getIoManager() {
            return null;
        }
    }

}
