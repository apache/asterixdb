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
package org.apache.asterix.app.external;

import static org.apache.asterix.api.http.server.UdfApiServlet.UDF_RESPONSE_TIMEOUT;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.app.message.DeleteUdfMessage;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.api.deployment.DeploymentId;

public class ExternalLibraryUtils {

    private ExternalLibraryUtils() {
    }

    public static void deleteDeployedUdf(ICCMessageBroker broker, ICcApplicationContext appCtx,
            DataverseName dataverseName, String lib) throws Exception {
        long reqId = broker.newRequestId();
        List<INcAddressedMessage> requests = new ArrayList<>();
        List<String> ncs = new ArrayList<>(appCtx.getClusterStateManager().getParticipantNodes());
        ncs.forEach(s -> requests.add(new DeleteUdfMessage(dataverseName, lib, reqId)));
        broker.sendSyncRequestToNCs(reqId, ncs, requests, UDF_RESPONSE_TIMEOUT);
        appCtx.getLibraryManager().deregister(dataverseName, lib);
        appCtx.getHcc().unDeployBinary(new DeploymentId(makeDeploymentId(dataverseName, lib)));
    }

    public static String makeDeploymentId(DataverseName dv, String resourceName) {
        List<String> dvParts = dv.getParts();
        dvParts.add(resourceName);
        DataverseName dvWithLibrarySuffix = DataverseName.create(dvParts);
        return dvWithLibrarySuffix.getCanonicalForm();
    }
}
