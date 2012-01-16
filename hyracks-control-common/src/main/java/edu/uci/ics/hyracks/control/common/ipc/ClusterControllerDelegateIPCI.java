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
package edu.uci.ics.hyracks.control.common.ipc;

import edu.uci.ics.hyracks.control.common.base.IClusterController;
import edu.uci.ics.hyracks.control.common.ipc.ClusterControllerFunctions.Function;
import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.IIPCI;

public class ClusterControllerDelegateIPCI implements IIPCI {
    private final IClusterController cc;

    public ClusterControllerDelegateIPCI(IClusterController cc) {
        this.cc = cc;
    }

    @Override
    public Object call(IIPCHandle caller, Object req) throws Exception {
        ClusterControllerFunctions.Function fn = (Function) req;
        switch (fn.getFunctionId()) {
            case REGISTER_NODE: {
                ClusterControllerFunctions.RegisterNodeFunction rnf = (ClusterControllerFunctions.RegisterNodeFunction) fn;
                return cc.registerNode(rnf.getNodeRegistration());
            }

            case UNREGISTER_NODE: {
                ClusterControllerFunctions.UnregisterNodeFunction unf = (ClusterControllerFunctions.UnregisterNodeFunction) fn;
                cc.unregisterNode(unf.getNodeId());
                return null;
            }

            case NODE_HEARTBEAT: {
                ClusterControllerFunctions.NodeHeartbeatFunction nhf = (ClusterControllerFunctions.NodeHeartbeatFunction) fn;
                cc.nodeHeartbeat(nhf.getNodeId(), nhf.getHeartbeatData());
                return null;
            }

            case NOTIFY_JOBLET_CLEANUP: {
                ClusterControllerFunctions.NotifyJobletCleanupFunction njcf = (ClusterControllerFunctions.NotifyJobletCleanupFunction) fn;
                cc.notifyJobletCleanup(njcf.getJobId(), njcf.getNodeId());
                return null;
            }

            case REPORT_PROFILE: {
                ClusterControllerFunctions.ReportProfileFunction rpf = (ClusterControllerFunctions.ReportProfileFunction) fn;
                cc.reportProfile(rpf.getNodeId(), rpf.getProfiles());
                return null;
            }

            case NOTIFY_TASK_COMPLETE: {
                ClusterControllerFunctions.NotifyTaskCompleteFunction ntcf = (ClusterControllerFunctions.NotifyTaskCompleteFunction) fn;
                cc.notifyTaskComplete(ntcf.getJobId(), ntcf.getTaskId(), ntcf.getNodeId(), ntcf.getStatistics());
                return null;
            }
            case NOTIFY_TASK_FAILURE: {
                ClusterControllerFunctions.NotifyTaskFailureFunction ntff = (ClusterControllerFunctions.NotifyTaskFailureFunction) fn;
                cc.notifyTaskFailure(ntff.getJobId(), ntff.getTaskId(), ntff.getDetails(), ntff.getDetails());
                return null;
            }

            case REGISTER_PARTITION_PROVIDER: {
                ClusterControllerFunctions.RegisterPartitionProviderFunction rppf = (ClusterControllerFunctions.RegisterPartitionProviderFunction) fn;
                cc.registerPartitionProvider(rppf.getPartitionDescriptor());
                return null;
            }

            case REGISTER_PARTITION_REQUEST: {
                ClusterControllerFunctions.RegisterPartitionRequestFunction rprf = (ClusterControllerFunctions.RegisterPartitionRequestFunction) fn;
                cc.registerPartitionRequest(rprf.getPartitionRequest());
                return null;
            }
        }
        throw new IllegalArgumentException("Unknown function: " + fn.getFunctionId());
    }
}