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
package edu.uci.ics.hyracks.control.cc.ipc;

import edu.uci.ics.hyracks.api.client.HyracksClientInterfaceFunctions;
import edu.uci.ics.hyracks.api.client.IHyracksClientInterface;
import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.IIPCI;

public class HyracksClientInterfaceDelegateIPCI implements IIPCI {
    private final IHyracksClientInterface hci;

    public HyracksClientInterfaceDelegateIPCI(IHyracksClientInterface hci) {
        this.hci = hci;
    }

    @Override
    public Object call(IIPCHandle caller, Object req) throws Exception {
        HyracksClientInterfaceFunctions.Function fn = (HyracksClientInterfaceFunctions.Function) req;
        switch (fn.getFunctionId()) {
            case GET_CLUSTER_CONTROLLER_INFO: {
                return hci.getClusterControllerInfo();
            }

            case CREATE_APPLICATION: {
                HyracksClientInterfaceFunctions.CreateApplicationFunction caf = (HyracksClientInterfaceFunctions.CreateApplicationFunction) fn;
                hci.createApplication(caf.getAppName());
                return null;
            }

            case START_APPLICATION: {
                HyracksClientInterfaceFunctions.StartApplicationFunction saf = (HyracksClientInterfaceFunctions.StartApplicationFunction) fn;
                hci.startApplication(saf.getAppName());
                return null;
            }

            case DESTROY_APPLICATION: {
                HyracksClientInterfaceFunctions.DestroyApplicationFunction daf = (HyracksClientInterfaceFunctions.DestroyApplicationFunction) fn;
                hci.destroyApplication(daf.getAppName());
                return null;
            }

            case CREATE_JOB: {
                HyracksClientInterfaceFunctions.CreateJobFunction cjf = (HyracksClientInterfaceFunctions.CreateJobFunction) fn;
                return hci.createJob(cjf.getAppName(), cjf.getJobSpec(), cjf.getJobFlags());
            }

            case GET_JOB_STATUS: {
                HyracksClientInterfaceFunctions.GetJobStatusFunction gjsf = (HyracksClientInterfaceFunctions.GetJobStatusFunction) fn;
                return hci.getJobStatus(gjsf.getJobId());
            }

            case START_JOB: {
                HyracksClientInterfaceFunctions.StartJobFunction sjf = (HyracksClientInterfaceFunctions.StartJobFunction) fn;
                hci.startJob(sjf.getJobId());
                return null;
            }

            case WAIT_FOR_COMPLETION: {
                HyracksClientInterfaceFunctions.WaitForCompletionFunction wfcf = (HyracksClientInterfaceFunctions.WaitForCompletionFunction) fn;
                hci.waitForCompletion(wfcf.getJobId());
                return null;
            }

            case GET_NODE_CONTROLLERS_INFO: {
                return hci.getNodeControllersInfo();
            }
        }
        throw new IllegalArgumentException("Unknown function " + fn.getFunctionId());
    }
}