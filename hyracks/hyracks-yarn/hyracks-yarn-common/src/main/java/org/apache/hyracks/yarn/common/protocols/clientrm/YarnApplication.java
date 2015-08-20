/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.yarn.common.protocols.clientrm;

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;

public class YarnApplication {
    private final YarnClientRMConnection crmc;

    private ApplicationSubmissionContext appCtx;

    private ContainerLaunchContext clCtx;

    YarnApplication(YarnClientRMConnection crmc, String appName) throws YarnRemoteException {
        this.crmc = crmc;
        appCtx = Records.newRecord(ApplicationSubmissionContext.class);
        appCtx.setApplicationId(getNewApplicationId(crmc));
        appCtx.setApplicationName(appName);
        clCtx = Records.newRecord(ContainerLaunchContext.class);
    }

    public ContainerLaunchContext getContainerLaunchContext() {
        return clCtx;
    }

    public void submit() throws YarnRemoteException {
        appCtx.setAMContainerSpec(clCtx);
        SubmitApplicationRequest appRequest = Records.newRecord(SubmitApplicationRequest.class);
        appRequest.setApplicationSubmissionContext(appCtx);
        crmc.getClientRMProtocol().submitApplication(appRequest);
    }

    private static ApplicationId getNewApplicationId(YarnClientRMConnection crmc) throws YarnRemoteException {
        GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
        GetNewApplicationResponse response = crmc.getClientRMProtocol().getNewApplication(request);

        return response.getApplicationId();
    }
}