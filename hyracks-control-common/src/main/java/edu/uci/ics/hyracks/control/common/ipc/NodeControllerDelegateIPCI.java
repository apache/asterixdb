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

import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.IIPCI;

public class NodeControllerDelegateIPCI implements IIPCI {
    private final INodeController nc;

    public NodeControllerDelegateIPCI(INodeController nc) {
        this.nc = nc;
    }

    @Override
    public Object call(IIPCHandle caller, Object req) throws Exception {
        NodeControllerFunctions.Function fn = (NodeControllerFunctions.Function) req;
        switch (fn.getFunctionId()) {
            case START_TASKS: {
                NodeControllerFunctions.StartTasksFunction stf = (NodeControllerFunctions.StartTasksFunction) fn;
                nc.startTasks(stf.getAppName(), stf.getJobId(), stf.getPlanBytes(), stf.getTaskDescriptors(),
                        stf.getConnectorPolicies());
                return null;
            }

            case ABORT_TASKS: {
                NodeControllerFunctions.AbortTasksFunction atf = (NodeControllerFunctions.AbortTasksFunction) fn;
                nc.abortTasks(atf.getJobId(), atf.getTasks());
                return null;
            }

            case CLEANUP_JOBLET: {
                NodeControllerFunctions.CleanupJobletFunction cjf = (NodeControllerFunctions.CleanupJobletFunction) fn;
                nc.cleanUpJoblet(cjf.getJobId(), cjf.getStatus());
                return null;
            }

            case CREATE_APPLICATION: {
                NodeControllerFunctions.CreateApplicationFunction caf = (NodeControllerFunctions.CreateApplicationFunction) fn;
                nc.createApplication(caf.getAppName(), caf.isDeployHar(), caf.getSerializedDistributedState());
                return null;
            }

            case DESTROY_APPLICATION: {
                NodeControllerFunctions.DestroyApplicationFunction daf = (NodeControllerFunctions.DestroyApplicationFunction) fn;
                nc.destroyApplication(daf.getAppName());
                return null;
            }

            case REPORT_PARTITION_AVAILABILITY: {
                NodeControllerFunctions.ReportPartitionAvailabilityFunction rpaf = (NodeControllerFunctions.ReportPartitionAvailabilityFunction) fn;
                nc.reportPartitionAvailability(rpaf.getPartitionId(), rpaf.getNetworkAddress());
                return null;
            }
        }
        throw new IllegalArgumentException("Unknown function: " + fn.getFunctionId());
    }
}