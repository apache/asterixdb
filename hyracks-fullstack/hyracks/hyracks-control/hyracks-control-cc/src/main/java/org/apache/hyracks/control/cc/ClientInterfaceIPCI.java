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

import org.apache.hyracks.api.client.HyracksClientInterfaceFunctions;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.DeployedJobSpecIdFactory;
import org.apache.hyracks.api.job.JobIdFactory;
import org.apache.hyracks.api.job.JobInfo;
import org.apache.hyracks.api.result.ResultJobRecord.Status;
import org.apache.hyracks.control.cc.work.CancelJobWork;
import org.apache.hyracks.control.cc.work.CliDeployBinaryWork;
import org.apache.hyracks.control.cc.work.CliUnDeployBinaryWork;
import org.apache.hyracks.control.cc.work.ClusterShutdownWork;
import org.apache.hyracks.control.cc.work.DeployJobSpecWork;
import org.apache.hyracks.control.cc.work.GetJobInfoWork;
import org.apache.hyracks.control.cc.work.GetJobStatusWork;
import org.apache.hyracks.control.cc.work.GetNodeControllersInfoWork;
import org.apache.hyracks.control.cc.work.GetNodeDetailsJSONWork;
import org.apache.hyracks.control.cc.work.GetResultDirectoryAddressWork;
import org.apache.hyracks.control.cc.work.GetResultMetadataWork;
import org.apache.hyracks.control.cc.work.GetResultPartitionLocationsWork;
import org.apache.hyracks.control.cc.work.GetResultStatusWork;
import org.apache.hyracks.control.cc.work.GetThreadDumpWork;
import org.apache.hyracks.control.cc.work.JobStartWork;
import org.apache.hyracks.control.cc.work.UndeployJobSpecWork;
import org.apache.hyracks.control.cc.work.WaitForJobCompletionWork;
import org.apache.hyracks.control.common.work.IPCResponder;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.IIPCI;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ClientInterfaceIPCI implements IIPCI {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ClusterControllerService ccs;
    private final JobIdFactory jobIdFactory;
    private final DeployedJobSpecIdFactory deployedJobSpecIdFactory;

    ClientInterfaceIPCI(final ClusterControllerService ccs, final JobIdFactory jobIdFactory) {
        this.ccs = ccs;
        this.jobIdFactory = jobIdFactory;
        this.deployedJobSpecIdFactory = ccs.getDeployedJobSpecIdFactory();
    }

    @Override
    public void deliverIncomingMessage(IIPCHandle handle, long mid, long rmid, Object payload, Exception exception) {
        HyracksClientInterfaceFunctions.Function fn = (HyracksClientInterfaceFunctions.Function) payload;
        switch (fn.getFunctionId()) {
            case GET_CLUSTER_CONTROLLER_INFO:
                try {
                    handle.send(mid, ccs.getClusterControllerInfo(), null);
                } catch (IPCException e) {
                    LOGGER.log(Level.WARN, "Error sending response to GET_CLUSTER_CONTROLLER_INFO request", e);
                }
                break;
            case GET_JOB_STATUS:
                HyracksClientInterfaceFunctions.GetJobStatusFunction gjsf =
                        (HyracksClientInterfaceFunctions.GetJobStatusFunction) fn;
                ccs.getWorkQueue().schedule(
                        new GetJobStatusWork(ccs.getJobManager(), gjsf.getJobId(), new IPCResponder<>(handle, mid)));
                break;
            case GET_JOB_INFO:
                HyracksClientInterfaceFunctions.GetJobInfoFunction gjif =
                        (HyracksClientInterfaceFunctions.GetJobInfoFunction) fn;
                ccs.getWorkQueue().schedule(new GetJobInfoWork(ccs.getJobManager(), gjif.getJobId(),
                        new IPCResponder<JobInfo>(handle, mid)));
                break;
            case DEPLOY_JOB:
                HyracksClientInterfaceFunctions.DeployJobSpecFunction djf =
                        (HyracksClientInterfaceFunctions.DeployJobSpecFunction) fn;
                ccs.getWorkQueue().schedule(new DeployJobSpecWork(ccs, djf.getACGGFBytes(),
                        deployedJobSpecIdFactory.create(), false, new IPCResponder<>(handle, mid)));
                break;
            case REDEPLOY_JOB:
                HyracksClientInterfaceFunctions.redeployJobSpecFunction udjsf =
                        (HyracksClientInterfaceFunctions.redeployJobSpecFunction) fn;
                ccs.getWorkQueue().schedule(new DeployJobSpecWork(ccs, udjsf.getACGGFBytes(),
                        udjsf.getDeployedJobSpecId(), true, new IPCResponder<>(handle, mid)));
                break;
            case UNDEPLOY_JOB:
                HyracksClientInterfaceFunctions.UndeployJobSpecFunction dsjf =
                        (HyracksClientInterfaceFunctions.UndeployJobSpecFunction) fn;
                ccs.getWorkQueue().schedule(
                        new UndeployJobSpecWork(ccs, dsjf.getDeployedJobSpecId(), new IPCResponder<>(handle, mid)));
                break;
            case CANCEL_JOB:
                HyracksClientInterfaceFunctions.CancelJobFunction cjf =
                        (HyracksClientInterfaceFunctions.CancelJobFunction) fn;
                ccs.getWorkQueue().schedule(
                        new CancelJobWork(ccs.getJobManager(), cjf.getJobId(), new IPCResponder<Void>(handle, mid)));
                break;
            case START_JOB:
                HyracksClientInterfaceFunctions.StartJobFunction sjf =
                        (HyracksClientInterfaceFunctions.StartJobFunction) fn;
                DeployedJobSpecId id = sjf.getDeployedJobSpecId();
                byte[] acggfBytes = null;
                if (id == null) {
                    //The job is new
                    acggfBytes = sjf.getACGGFBytes();
                }
                ccs.getWorkQueue().schedule(new JobStartWork(ccs, sjf.getDeploymentId(), acggfBytes, sjf.getJobFlags(),
                        jobIdFactory, sjf.getJobParameters(), new IPCResponder<>(handle, mid), id));
                break;
            case GET_RESULT_DIRECTORY_ADDRESS:
                ccs.getWorkQueue().schedule(
                        new GetResultDirectoryAddressWork(ccs, new IPCResponder<NetworkAddress>(handle, mid)));
                break;
            case GET_RESULT_STATUS:
                HyracksClientInterfaceFunctions.GetResultStatusFunction gdrsf =
                        (HyracksClientInterfaceFunctions.GetResultStatusFunction) fn;
                ccs.getWorkQueue().schedule(new GetResultStatusWork(ccs, gdrsf.getJobId(), gdrsf.getResultSetId(),
                        new IPCResponder<Status>(handle, mid)));
                break;
            case GET_RESULT_LOCATIONS:
                HyracksClientInterfaceFunctions.GetResultLocationsFunction gdrlf =
                        (HyracksClientInterfaceFunctions.GetResultLocationsFunction) fn;
                ccs.getWorkQueue().schedule(new GetResultPartitionLocationsWork(ccs, gdrlf.getJobId(),
                        gdrlf.getResultSetId(), gdrlf.getKnownRecords(), new IPCResponder<>(handle, mid)));
                break;
            case GET_RESULT_METADATA:
                HyracksClientInterfaceFunctions.GetResultMetadataFunction grmf =
                        (HyracksClientInterfaceFunctions.GetResultMetadataFunction) fn;
                ccs.getWorkQueue().schedule(new GetResultMetadataWork(ccs, grmf.getJobId(), grmf.getResultSetId(),
                        new IPCResponder<>(handle, mid)));
                break;
            case WAIT_FOR_COMPLETION:
                HyracksClientInterfaceFunctions.WaitForCompletionFunction wfcf =
                        (HyracksClientInterfaceFunctions.WaitForCompletionFunction) fn;
                ccs.getWorkQueue()
                        .schedule(new WaitForJobCompletionWork(ccs, wfcf.getJobId(), new IPCResponder<>(handle, mid)));
                break;
            case GET_NODE_CONTROLLERS_INFO:
                ccs.getWorkQueue().schedule(
                        new GetNodeControllersInfoWork(ccs.getNodeManager(), new IPCResponder<>(handle, mid)));
                break;
            case GET_CLUSTER_TOPOLOGY:
                try {
                    handle.send(mid, ccs.getCCContext().getClusterTopology(), null);
                } catch (IPCException e) {
                    LOGGER.log(Level.WARN, "Error sending response to GET_CLUSTER_TOPOLOGY request", e);
                }
                break;
            case CLI_DEPLOY_BINARY:
                HyracksClientInterfaceFunctions.CliDeployBinaryFunction dbf =
                        (HyracksClientInterfaceFunctions.CliDeployBinaryFunction) fn;
                ccs.getWorkQueue().schedule(new CliDeployBinaryWork(ccs, dbf.getBinaryURLs(), dbf.getDeploymentId(),
                        dbf.isExtractFromArchive(), new IPCResponder<>(handle, mid)));
                break;
            case CLI_UNDEPLOY_BINARY:
                HyracksClientInterfaceFunctions.CliUnDeployBinaryFunction udbf =
                        (HyracksClientInterfaceFunctions.CliUnDeployBinaryFunction) fn;
                ccs.getWorkQueue().schedule(
                        new CliUnDeployBinaryWork(ccs, udbf.getDeploymentId(), new IPCResponder<>(handle, mid)));
                break;
            case CLUSTER_SHUTDOWN:
                HyracksClientInterfaceFunctions.ClusterShutdownFunction csf =
                        (HyracksClientInterfaceFunctions.ClusterShutdownFunction) fn;
                ccs.getWorkQueue().schedule(
                        new ClusterShutdownWork(ccs, csf.isTerminateNCService(), new IPCResponder<>(handle, mid)));
                break;
            case GET_NODE_DETAILS_JSON:
                HyracksClientInterfaceFunctions.GetNodeDetailsJSONFunction gndjf =
                        (HyracksClientInterfaceFunctions.GetNodeDetailsJSONFunction) fn;
                ccs.getWorkQueue()
                        .schedule(new GetNodeDetailsJSONWork(ccs.getNodeManager(), ccs.getCCConfig(), gndjf.getNodeId(),
                                gndjf.isIncludeStats(), gndjf.isIncludeConfig(), new IPCResponder<>(handle, mid)));
                break;
            case THREAD_DUMP:
                HyracksClientInterfaceFunctions.ThreadDumpFunction tdf =
                        (HyracksClientInterfaceFunctions.ThreadDumpFunction) fn;
                ccs.getWorkQueue()
                        .schedule(new GetThreadDumpWork(ccs, tdf.getNode(), new IPCResponder<String>(handle, mid)));
                break;
            default:
                try {
                    handle.send(mid, null, new IllegalArgumentException("Unknown function " + fn.getFunctionId()));
                } catch (IPCException e) {
                    LOGGER.log(Level.WARN, "Error sending Unknown function response", e);
                }
        }
    }
}
