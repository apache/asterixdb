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
package org.apache.hyracks.control.nc;

import org.apache.hyracks.control.common.ipc.CCNCFunctions;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.StateDumpRequestFunction;
import org.apache.hyracks.control.nc.task.HeartbeatAckTask;
import org.apache.hyracks.control.nc.task.PingTask;
import org.apache.hyracks.control.nc.task.ShutdownTask;
import org.apache.hyracks.control.nc.task.ThreadDumpTask;
import org.apache.hyracks.control.nc.work.AbortAllJobsWork;
import org.apache.hyracks.control.nc.work.AbortTasksWork;
import org.apache.hyracks.control.nc.work.ApplicationMessageWork;
import org.apache.hyracks.control.nc.work.CleanupJobletWork;
import org.apache.hyracks.control.nc.work.DeployBinaryWork;
import org.apache.hyracks.control.nc.work.DeployJobSpecWork;
import org.apache.hyracks.control.nc.work.ReportPartitionAvailabilityWork;
import org.apache.hyracks.control.nc.work.StartTasksWork;
import org.apache.hyracks.control.nc.work.StateDumpWork;
import org.apache.hyracks.control.nc.work.UnDeployBinaryWork;
import org.apache.hyracks.control.nc.work.UndeployJobSpecWork;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.IIPCI;

/**
 * Interprocess communication in a node controller
 * This class must be refactored with each function carrying its own implementation
 */
final class NodeControllerIPCI implements IIPCI {
    private final NodeControllerService ncs;

    /**
     * @param nodeControllerService
     */
    NodeControllerIPCI(NodeControllerService nodeControllerService) {
        ncs = nodeControllerService;
    }

    @Override
    public void deliverIncomingMessage(final IIPCHandle handle, long mid, long rmid, Object payload,
            Exception exception) {
        CCNCFunctions.Function fn = (CCNCFunctions.Function) payload;
        switch (fn.getFunctionId()) {
            case SEND_APPLICATION_MESSAGE:
                CCNCFunctions.SendApplicationMessageFunction amf = (CCNCFunctions.SendApplicationMessageFunction) fn;
                ncs.getWorkQueue().schedule(
                        new ApplicationMessageWork(ncs, amf.getMessage(), amf.getDeploymentId(), amf.getNodeId()));
                return;
            case START_TASKS:
                CCNCFunctions.StartTasksFunction stf = (CCNCFunctions.StartTasksFunction) fn;
                ncs.getWorkQueue()
                        .schedule(new StartTasksWork(ncs, stf.getDeploymentId(), stf.getJobId(), stf.getPlanBytes(),
                                stf.getTaskDescriptors(), stf.getConnectorPolicies(), stf.getFlags(),
                                stf.getJobParameters(), stf.getDeployedJobSpecId(), stf.getJobStartTime()));
                return;
            case ABORT_TASKS:
                CCNCFunctions.AbortTasksFunction atf = (CCNCFunctions.AbortTasksFunction) fn;
                ncs.getWorkQueue().schedule(new AbortTasksWork(ncs, atf.getJobId(), atf.getTasks()));
                return;
            case ABORT_ALL_JOBS:
                CCNCFunctions.AbortCCJobsFunction aajf = (CCNCFunctions.AbortCCJobsFunction) fn;
                ncs.getWorkQueue().schedule(new AbortAllJobsWork(ncs, aajf.getCcId()));
                return;
            case CLEANUP_JOBLET:
                CCNCFunctions.CleanupJobletFunction cjf = (CCNCFunctions.CleanupJobletFunction) fn;
                ncs.getWorkQueue().schedule(new CleanupJobletWork(ncs, cjf.getJobId(), cjf.getStatus()));
                return;
            case REPORT_PARTITION_AVAILABILITY:
                CCNCFunctions.ReportPartitionAvailabilityFunction rpaf =
                        (CCNCFunctions.ReportPartitionAvailabilityFunction) fn;
                ncs.getWorkQueue().schedule(
                        new ReportPartitionAvailabilityWork(ncs, rpaf.getPartitionId(), rpaf.getNetworkAddress()));
                return;
            case NODE_REGISTRATION_RESULT:
                CCNCFunctions.NodeRegistrationResult nrrf = (CCNCFunctions.NodeRegistrationResult) fn;
                ncs.setNodeRegistrationResult(nrrf.getNodeParameters(), nrrf.getException());
                return;

            case GET_NODE_CONTROLLERS_INFO_RESPONSE:
                CCNCFunctions.GetNodeControllersInfoResponseFunction gncirf =
                        (CCNCFunctions.GetNodeControllersInfoResponseFunction) fn;
                ncs.setNodeControllersInfo(gncirf.getNodeControllerInfos());
                return;

            case DEPLOY_BINARY:
                CCNCFunctions.DeployBinaryFunction dbf = (CCNCFunctions.DeployBinaryFunction) fn;
                ncs.getWorkQueue().schedule(new DeployBinaryWork(ncs, dbf.getDeploymentId(), dbf.getBinaryURLs(),
                        dbf.getCcId(), dbf.isExtractFromArchive()));
                return;

            case UNDEPLOY_BINARY:
                CCNCFunctions.UnDeployBinaryFunction ndbf = (CCNCFunctions.UnDeployBinaryFunction) fn;
                ncs.getWorkQueue().schedule(new UnDeployBinaryWork(ncs, ndbf.getDeploymentId(), ndbf.getCcId()));
                return;

            case DEPLOY_JOB:
                CCNCFunctions.DeployJobSpecFunction djf = (CCNCFunctions.DeployJobSpecFunction) fn;
                ncs.getWorkQueue().schedule(new DeployJobSpecWork(ncs, djf.getDeployedJobSpecId(), djf.getacgBytes(),
                        djf.getUpsert(), djf.getCcId()));
                return;

            case UNDEPLOY_JOB:
                CCNCFunctions.UndeployJobSpecFunction dsjf = (CCNCFunctions.UndeployJobSpecFunction) fn;
                ncs.getWorkQueue().schedule(new UndeployJobSpecWork(ncs, dsjf.getDeployedJobSpecId(), dsjf.getCcId()));
                return;

            case STATE_DUMP_REQUEST:
                final CCNCFunctions.StateDumpRequestFunction dsrf = (StateDumpRequestFunction) fn;
                ncs.getWorkQueue().schedule(new StateDumpWork(ncs, dsrf.getStateDumpId(), dsrf.getCcId()));
                return;

            case SHUTDOWN_REQUEST:
                final CCNCFunctions.ShutdownRequestFunction sdrf = (CCNCFunctions.ShutdownRequestFunction) fn;
                ncs.getExecutor().submit(new ShutdownTask(sdrf.isTerminateNCService()));
                return;

            case THREAD_DUMP_REQUEST:
                final CCNCFunctions.ThreadDumpRequestFunction tdrf = (CCNCFunctions.ThreadDumpRequestFunction) fn;
                ncs.getExecutor().submit(new ThreadDumpTask(ncs, tdrf.getRequestId(), tdrf.getCcId()));
                return;

            case PING_REQUEST:
                final CCNCFunctions.PingFunction pcf = (CCNCFunctions.PingFunction) fn;
                ncs.getExecutor().submit(new PingTask(ncs, pcf.getCcId()));
                return;

            case NODE_HEARTBEAT_ACK:
                final CCNCFunctions.NodeHeartbeatAckFunction nbaf = (CCNCFunctions.NodeHeartbeatAckFunction) fn;
                ncs.getExecutor().submit(new HeartbeatAckTask(ncs, nbaf.getCcId(), nbaf.getException()));
                return;

            default:
                throw new IllegalArgumentException("Unknown function: " + fn.getFunctionId());
        }

    }
}