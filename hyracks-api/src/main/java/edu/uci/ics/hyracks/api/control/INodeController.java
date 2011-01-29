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
package edu.uci.ics.hyracks.api.control;

import java.rmi.Remote;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import edu.uci.ics.hyracks.api.comm.Endpoint;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.PortInstanceId;

public interface INodeController extends Remote {
    public String getId() throws Exception;

    public NCConfig getConfiguration() throws Exception;

    public NodeCapability getNodeCapability() throws Exception;

    public Map<PortInstanceId, Endpoint> initializeJobletPhase1(String appName, UUID jobId, int attempt, byte[] plan,
            UUID stageId, Map<ActivityNodeId, Set<Integer>> tasks, Map<OperatorDescriptorId, Integer> opNumPartitions)
            throws Exception;

    public void initializeJobletPhase2(String appName, UUID jobId, byte[] plan, UUID stageId,
            Map<ActivityNodeId, Set<Integer>> tasks, Map<OperatorDescriptorId, Integer> opNumPartitions,
            Map<PortInstanceId, Endpoint> globalPortMap) throws Exception;

    public void commitJobletInitialization(UUID jobId, UUID stageId) throws Exception;

    public void abortJoblet(UUID jobId, int attempt) throws Exception;

    public void cleanUpJob(UUID jobId) throws Exception;

    public void startStage(UUID jobId, UUID stageId) throws Exception;

    public void notifyRegistration(IClusterController ccs) throws Exception;

    public void createApplication(String appName, boolean deployHar, byte[] serializedDistributedState)
            throws Exception;

    public void destroyApplication(String appName) throws Exception;
}