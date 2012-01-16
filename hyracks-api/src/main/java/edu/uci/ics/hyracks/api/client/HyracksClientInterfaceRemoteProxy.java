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
package edu.uci.ics.hyracks.api.client;

import java.util.EnumSet;
import java.util.Map;

import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.SyncRMI;

public class HyracksClientInterfaceRemoteProxy implements IHyracksClientInterface {
    private final IIPCHandle ipcHandle;

    public HyracksClientInterfaceRemoteProxy(IIPCHandle ipcHandle) {
        this.ipcHandle = ipcHandle;
    }

    @Override
    public ClusterControllerInfo getClusterControllerInfo() throws Exception {
        SyncRMI sync = new SyncRMI();
        HyracksClientInterfaceFunctions.GetClusterControllerInfoFunction gccif = new HyracksClientInterfaceFunctions.GetClusterControllerInfoFunction();
        return (ClusterControllerInfo) sync.call(ipcHandle, gccif);
    }

    @Override
    public void createApplication(String appName) throws Exception {
        SyncRMI sync = new SyncRMI();
        HyracksClientInterfaceFunctions.CreateApplicationFunction caf = new HyracksClientInterfaceFunctions.CreateApplicationFunction(
                appName);
        sync.call(ipcHandle, caf);
    }

    @Override
    public void startApplication(String appName) throws Exception {
        SyncRMI sync = new SyncRMI();
        HyracksClientInterfaceFunctions.StartApplicationFunction saf = new HyracksClientInterfaceFunctions.StartApplicationFunction(
                appName);
        sync.call(ipcHandle, saf);
    }

    @Override
    public void destroyApplication(String appName) throws Exception {
        SyncRMI sync = new SyncRMI();
        HyracksClientInterfaceFunctions.DestroyApplicationFunction daf = new HyracksClientInterfaceFunctions.DestroyApplicationFunction(
                appName);
        sync.call(ipcHandle, daf);
    }

    @Override
    public JobId createJob(String appName, byte[] jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        SyncRMI sync = new SyncRMI();
        HyracksClientInterfaceFunctions.CreateJobFunction cjf = new HyracksClientInterfaceFunctions.CreateJobFunction(
                appName, jobSpec, jobFlags);
        return (JobId) sync.call(ipcHandle, cjf);
    }

    @Override
    public JobStatus getJobStatus(JobId jobId) throws Exception {
        SyncRMI sync = new SyncRMI();
        HyracksClientInterfaceFunctions.GetJobStatusFunction gjsf = new HyracksClientInterfaceFunctions.GetJobStatusFunction(
                jobId);
        return (JobStatus) sync.call(ipcHandle, gjsf);
    }

    @Override
    public void startJob(JobId jobId) throws Exception {
        SyncRMI sync = new SyncRMI();
        HyracksClientInterfaceFunctions.StartJobFunction sjf = new HyracksClientInterfaceFunctions.StartJobFunction(
                jobId);
        sync.call(ipcHandle, sjf);
    }

    @Override
    public void waitForCompletion(JobId jobId) throws Exception {
        SyncRMI sync = new SyncRMI();
        HyracksClientInterfaceFunctions.WaitForCompletionFunction wfcf = new HyracksClientInterfaceFunctions.WaitForCompletionFunction(
                jobId);
        sync.call(ipcHandle, wfcf);
    }

    @Override
    public Map<String, NodeControllerInfo> getNodeControllersInfo() throws Exception {
        SyncRMI sync = new SyncRMI();
        HyracksClientInterfaceFunctions.GetNodeControllersInfoFunction gncif = new HyracksClientInterfaceFunctions.GetNodeControllersInfoFunction();
        return (Map<String, NodeControllerInfo>) sync.call(ipcHandle, gncif);
    }
}