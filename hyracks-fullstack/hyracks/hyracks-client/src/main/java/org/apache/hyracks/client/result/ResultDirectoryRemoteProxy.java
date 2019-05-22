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

import org.apache.hyracks.api.client.HyracksClientInterfaceFunctions;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.IResultDirectory;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.api.result.ResultDirectoryRecord;
import org.apache.hyracks.api.result.ResultJobRecord.Status;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.RPCInterface;

//TODO(madhusudancs): Should this implementation be moved to org.apache.hyracks.client?
public class ResultDirectoryRemoteProxy implements IResultDirectory {
    private final IIPCHandle ipcHandle;

    private final RPCInterface rpci;

    public ResultDirectoryRemoteProxy(IIPCHandle ipcHandle, RPCInterface rpci) {
        this.ipcHandle = ipcHandle;
        this.rpci = rpci;
    }

    @Override
    public Status getResultStatus(JobId jobId, ResultSetId rsId) throws Exception {
        HyracksClientInterfaceFunctions.GetResultStatusFunction gdrlf =
                new HyracksClientInterfaceFunctions.GetResultStatusFunction(jobId, rsId);
        return (Status) rpci.call(ipcHandle, gdrlf);
    }

    @Override
    public ResultDirectoryRecord[] getResultLocations(JobId jobId, ResultSetId rsId,
            ResultDirectoryRecord[] knownRecords) throws Exception {
        HyracksClientInterfaceFunctions.GetResultLocationsFunction gdrlf =
                new HyracksClientInterfaceFunctions.GetResultLocationsFunction(jobId, rsId, knownRecords);
        return (ResultDirectoryRecord[]) rpci.call(ipcHandle, gdrlf);
    }

    @Override
    public IResultMetadata getResultMetadata(JobId jobId, ResultSetId rsId) throws Exception {
        HyracksClientInterfaceFunctions.GetResultMetadataFunction grmf =
                new HyracksClientInterfaceFunctions.GetResultMetadataFunction(jobId, rsId);
        return (IResultMetadata) rpci.call(ipcHandle, grmf);
    }
}
