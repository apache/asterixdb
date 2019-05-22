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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.network.ISocketChannelFactory;
import org.apache.hyracks.api.result.IResultDirectory;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.api.result.ResultDirectoryRecord;
import org.apache.hyracks.api.result.ResultJobRecord.Status;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.RPCInterface;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.hyracks.ipc.impl.IPCSystem;
import org.apache.hyracks.ipc.impl.JavaSerializationBasedPayloadSerializerDeserializer;

//TODO(madhusudancs): Should this implementation be moved to org.apache.hyracks.client?
public class ResultDirectory implements IResultDirectory {
    private final IPCSystem ipc;
    private final IResultDirectory remoteResultDirectory;

    public ResultDirectory(String resultHost, int resultPort, ISocketChannelFactory socketChannelFactory)
            throws IOException, IPCException {
        RPCInterface rpci = new RPCInterface();
        ipc = new IPCSystem(new InetSocketAddress(0), socketChannelFactory, rpci,
                new JavaSerializationBasedPayloadSerializerDeserializer());
        ipc.start();
        IIPCHandle ddsIpchandle = ipc.getReconnectingHandle(new InetSocketAddress(resultHost, resultPort));
        this.remoteResultDirectory = new ResultDirectoryRemoteProxy(ddsIpchandle, rpci);
    }

    @Override
    public Status getResultStatus(JobId jobId, ResultSetId rsId) throws Exception {
        return remoteResultDirectory.getResultStatus(jobId, rsId);
    }

    @Override
    public ResultDirectoryRecord[] getResultLocations(JobId jobId, ResultSetId rsId,
            ResultDirectoryRecord[] knownRecords) throws Exception {
        return remoteResultDirectory.getResultLocations(jobId, rsId, knownRecords);
    }

    @Override
    public IResultMetadata getResultMetadata(JobId jobId, ResultSetId rsId) throws Exception {
        return remoteResultDirectory.getResultMetadata(jobId, rsId);
    }
}
