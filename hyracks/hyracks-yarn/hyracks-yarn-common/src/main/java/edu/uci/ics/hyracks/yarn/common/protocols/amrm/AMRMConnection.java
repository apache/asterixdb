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
package edu.uci.ics.hyracks.yarn.common.protocols.amrm;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class AMRMConnection {
    private final YarnConfiguration config;

    private final ApplicationAttemptId appAttemptId;

    private final AMRMProtocol amrmp;

    public AMRMConnection(YarnConfiguration config) {
        this.config = config;
        Map<String, String> envs = System.getenv();
        String containerIdString = envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV);
        if (containerIdString == null) {
            throw new IllegalArgumentException("ContainerId not set in the environment");
        }
        ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
        appAttemptId = containerId.getApplicationAttemptId();
        InetSocketAddress rmAddress = NetUtils.createSocketAddr(config.get(YarnConfiguration.RM_SCHEDULER_ADDRESS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS));
        YarnRPC rpc = YarnRPC.create(config);

        amrmp = (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, config);
    }

    public ApplicationAttemptId getApplicationAttemptId() {
        return appAttemptId;
    }

    public AMRMProtocol getAMRMProtocol() {
        return amrmp;
    }
}