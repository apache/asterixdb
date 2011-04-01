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
package edu.uci.ics.hyracks.control.common.base;

import java.rmi.Remote;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.TaskProfile;

public interface IClusterController extends Remote {
    public NodeParameters registerNode(INodeController nodeController) throws Exception;

    public void unregisterNode(INodeController nodeController) throws Exception;

    public void notifyTaskComplete(UUID jobId, TaskAttemptId taskId, String nodeId, TaskProfile statistics) throws Exception;

    public void notifyTaskFailure(UUID jobId, TaskAttemptId taskId, String nodeId, Exception e) throws Exception;

    public void nodeHeartbeat(String id) throws Exception;

    public void reportProfile(String id, List<JobProfile> profiles) throws Exception;

    public void registerPartitionProvider(PartitionId pid, NetworkAddress address) throws Exception;

    public void registerPartitionRequest(Collection<PartitionId> requiredPartitionIds, String nodeId) throws Exception;
}