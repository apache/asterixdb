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
package org.apache.hyracks.control.nc.partitions;

import java.util.concurrent.ExecutorService;

import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.comm.channels.NetworkOutputChannel;
import org.apache.hyracks.control.nc.Joblet;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.net.protocols.muxdemux.AbstractChannelWriteInterface;

public class PartitionFileReaderUtil {

    private PartitionFileReaderUtil() {
    }

    public static void writeFileToChannel(NodeControllerService ncs, NetworkOutputChannel noc, JobId jobId,
            long fileId) {
        Joblet joblet = ncs.getJobletMap().get(jobId);
        if (joblet == null) {
            noc.abort(AbstractChannelWriteInterface.REMOTE_ERROR_CODE);
            return;
        }
        IStateObject stateObject = joblet.getEnvironment().getStateObject(new JobFileState.JobFileId(fileId));
        if (!(stateObject instanceof JobFileState)) {
            noc.abort(AbstractChannelWriteInterface.REMOTE_ERROR_CODE);
            return;
        }
        JobFileState fileState = (JobFileState) stateObject;
        FileReference fileRef = fileState.getFileRef();
        if (!fileRef.getFile().exists()) {
            noc.abort(AbstractChannelWriteInterface.REMOTE_ERROR_CODE);
            return;
        }
        ExecutorService executor = ncs.getExecutor();
        noc.setFrameSize(joblet.getInitialFrameSize());
        executor.execute(new PartitionFileReader(joblet, fileRef, ncs.getIoManager(), noc, true));
    }
}
