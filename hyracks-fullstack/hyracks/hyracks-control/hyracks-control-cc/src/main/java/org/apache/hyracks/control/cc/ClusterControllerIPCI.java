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
package org.apache.hyracks.control.cc;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.control.cc.work.ApplicationMessageWork;
import org.apache.hyracks.control.cc.work.DeployedJobFailureWork;
import org.apache.hyracks.control.cc.work.GetNodeControllersInfoWork;
import org.apache.hyracks.control.cc.work.JobletCleanupNotificationWork;
import org.apache.hyracks.control.cc.work.NodeHeartbeatWork;
import org.apache.hyracks.control.cc.work.NotifyDeployBinaryWork;
import org.apache.hyracks.control.cc.work.NotifyShutdownWork;
import org.apache.hyracks.control.cc.work.NotifyStateDumpResponse;
import org.apache.hyracks.control.cc.work.NotifyThreadDumpResponse;
import org.apache.hyracks.control.cc.work.RegisterNodeWork;
import org.apache.hyracks.control.cc.work.RegisterPartitionAvailibilityWork;
import org.apache.hyracks.control.cc.work.RegisterPartitionRequestWork;
import org.apache.hyracks.control.cc.work.RegisterResultPartitionLocationWork;
import org.apache.hyracks.control.cc.work.ReportProfilesWork;
import org.apache.hyracks.control.cc.work.ReportResultPartitionWriteCompletionWork;
import org.apache.hyracks.control.cc.work.TaskCompleteWork;
import org.apache.hyracks.control.cc.work.TaskFailureWork;
import org.apache.hyracks.control.cc.work.UnregisterNodeWork;
import org.apache.hyracks.control.common.ipc.CCNCFunctions;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.Function;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.ShutdownResponseFunction;
import org.apache.hyracks.control.common.ipc.CCNCFunctions.StateDumpResponseFunction;
import org.apache.hyracks.control.common.work.IPCResponder;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.IIPCI;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ClusterControllerIPCI implements IIPCI {
    private static final Logger LOGGER = LogManager.getLogger();
    private final ClusterControllerService ccs;

    ClusterControllerIPCI(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public void deliverIncomingMessage(final IIPCHandle handle, long mid, long rmid, Object payload,
            Exception exception) {
        CCNCFunctions.Function fn = (Function) payload;
        switch (fn.getFunctionId()) {
            case REGISTER_NODE:
                CCNCFunctions.RegisterNodeFunction rnf = (CCNCFunctions.RegisterNodeFunction) fn;
                ccs.getWorkQueue()
                        .schedule(new RegisterNodeWork(ccs, rnf.getNodeRegistration(), rnf.getRegistrationId()));
                break;
            case UNREGISTER_NODE:
                CCNCFunctions.UnregisterNodeFunction unf = (CCNCFunctions.UnregisterNodeFunction) fn;
                ccs.getWorkQueue().schedule(new UnregisterNodeWork(ccs.getNodeManager(), unf.getNodeId()));
                break;
            case NODE_HEARTBEAT:
                processNodeHeartbeat(ccs, fn);
                break;
            case NOTIFY_JOBLET_CLEANUP:
                CCNCFunctions.NotifyJobletCleanupFunction njcf = (CCNCFunctions.NotifyJobletCleanupFunction) fn;
                ccs.getWorkQueue().schedule(new JobletCleanupNotificationWork(ccs, njcf.getJobId(), njcf.getNodeId()));
                break;
            case NOTIFY_DEPLOY_BINARY:
                CCNCFunctions.NotifyDeployBinaryFunction ndbf = (CCNCFunctions.NotifyDeployBinaryFunction) fn;
                ccs.getWorkQueue().schedule(new NotifyDeployBinaryWork(ccs, ndbf.getDeploymentId(), ndbf.getNodeId(),
                        ndbf.getDeploymentStatus()));
                break;
            case REPORT_PROFILE:
                CCNCFunctions.ReportProfileFunction rpf = (CCNCFunctions.ReportProfileFunction) fn;
                ccs.getWorkQueue().schedule(new ReportProfilesWork(ccs.getJobManager(), rpf.getProfiles()));
                break;
            case NOTIFY_TASK_COMPLETE:
                CCNCFunctions.NotifyTaskCompleteFunction ntcf = (CCNCFunctions.NotifyTaskCompleteFunction) fn;
                ccs.getWorkQueue().schedule(new TaskCompleteWork(ccs, ntcf.getJobId(), ntcf.getTaskId(),
                        ntcf.getNodeId(), ntcf.getStatistics()));
                break;
            case NOTIFY_TASK_FAILURE:
                CCNCFunctions.NotifyTaskFailureFunction ntff = (CCNCFunctions.NotifyTaskFailureFunction) fn;
                ccs.getWorkQueue().schedule(new TaskFailureWork(ccs, ntff.getJobId(), ntff.getTaskId(),
                        ntff.getNodeId(), ntff.getExceptions()));
                break;
            case DEPLOYED_JOB_FAILURE:
                CCNCFunctions.ReportDeployedJobSpecFailureFunction rdjf =
                        (CCNCFunctions.ReportDeployedJobSpecFailureFunction) fn;
                ccs.getWorkQueue().schedule(new DeployedJobFailureWork(rdjf.getDeployedJobSpecId(), rdjf.getNodeId()));
                break;
            case REGISTER_PARTITION_PROVIDER:
                CCNCFunctions.RegisterPartitionProviderFunction rppf =
                        (CCNCFunctions.RegisterPartitionProviderFunction) fn;
                ccs.getWorkQueue().schedule(new RegisterPartitionAvailibilityWork(ccs, rppf.getPartitionDescriptor()));
                break;
            case REGISTER_PARTITION_REQUEST:
                CCNCFunctions.RegisterPartitionRequestFunction rprf =
                        (CCNCFunctions.RegisterPartitionRequestFunction) fn;
                ccs.getWorkQueue().schedule(new RegisterPartitionRequestWork(ccs, rprf.getPartitionRequest()));
                break;
            case REGISTER_RESULT_PARTITION_LOCATION:
                CCNCFunctions.RegisterResultPartitionLocationFunction rrplf =
                        (CCNCFunctions.RegisterResultPartitionLocationFunction) fn;
                ccs.getWorkQueue()
                        .schedule(new RegisterResultPartitionLocationWork(ccs, rrplf.getJobId(), rrplf.getResultSetId(),
                                rrplf.getMetadata(), rrplf.getEmptyResult(), rrplf.getPartition(),
                                rrplf.getNPartitions(), rrplf.getNetworkAddress()));
                break;
            case REPORT_RESULT_PARTITION_WRITE_COMPLETION:
                CCNCFunctions.ReportResultPartitionWriteCompletionFunction rrpwc =
                        (CCNCFunctions.ReportResultPartitionWriteCompletionFunction) fn;
                ccs.getWorkQueue().schedule(new ReportResultPartitionWriteCompletionWork(ccs, rrpwc.getJobId(),
                        rrpwc.getResultSetId(), rrpwc.getPartition()));
                break;
            case SEND_APPLICATION_MESSAGE:
                CCNCFunctions.SendApplicationMessageFunction rsf = (CCNCFunctions.SendApplicationMessageFunction) fn;
                ccs.getWorkQueue().schedule(
                        new ApplicationMessageWork(ccs, rsf.getMessage(), rsf.getDeploymentId(), rsf.getNodeId()));
                break;
            case GET_NODE_CONTROLLERS_INFO:
                ccs.getWorkQueue().schedule(new GetNodeControllersInfoWork(ccs.getNodeManager(),
                        new IResultCallback<Map<String, NodeControllerInfo>>() {
                            @Override
                            public void setValue(Map<String, NodeControllerInfo> result) {
                                new IPCResponder<CCNCFunctions.GetNodeControllersInfoResponseFunction>(handle, -1)
                                        .setValue(new CCNCFunctions.GetNodeControllersInfoResponseFunction(result));
                            }

                            @Override
                            public void setException(Exception e) {
                            }
                        }));
                break;
            case STATE_DUMP_RESPONSE:
                CCNCFunctions.StateDumpResponseFunction dsrf = (StateDumpResponseFunction) fn;
                ccs.getWorkQueue().schedule(
                        new NotifyStateDumpResponse(ccs, dsrf.getNodeId(), dsrf.getStateDumpId(), dsrf.getState()));
                break;
            case SHUTDOWN_RESPONSE:
                CCNCFunctions.ShutdownResponseFunction sdrf = (ShutdownResponseFunction) fn;
                ccs.getWorkQueue().schedule(new NotifyShutdownWork(ccs, sdrf.getNodeId()));
                break;
            case THREAD_DUMP_RESPONSE:
                CCNCFunctions.ThreadDumpResponseFunction tdrf = (CCNCFunctions.ThreadDumpResponseFunction) fn;
                ccs.getWorkQueue()
                        .schedule(new NotifyThreadDumpResponse(ccs, tdrf.getRequestId(), tdrf.getThreadDumpJSON()));
                break;
            case PING_RESPONSE:
                CCNCFunctions.PingResponseFunction prf = (CCNCFunctions.PingResponseFunction) fn;
                LOGGER.debug("Received ping response from node {}", prf.getNodeId());
                break;
            default:
                LOGGER.warn("Unknown function: " + fn.getFunctionId());
        }
    }

    private static void processNodeHeartbeat(ClusterControllerService ccs, CCNCFunctions.Function fn) {
        final ExecutorService executor = ccs.getExecutor();
        if (executor != null) {
            CCNCFunctions.NodeHeartbeatFunction nhf = (CCNCFunctions.NodeHeartbeatFunction) fn;
            executor.execute(new NodeHeartbeatWork(ccs, nhf.getNodeId(), nhf.getHeartbeatData(), nhf.getNcAddress()));
        }
    }
}
