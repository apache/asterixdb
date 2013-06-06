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

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;
import org.apache.hadoop.yarn.util.Records;

public class YarnClientRMConnection {
    private final YarnConfiguration config;

    private final ClientRMProtocol crmp;

    public YarnClientRMConnection(YarnConfiguration config) {
        this.config = config;
        InetSocketAddress remoteAddress = NetUtils.createSocketAddr(config.get(YarnConfiguration.RM_ADDRESS,
                YarnConfiguration.DEFAULT_RM_ADDRESS));
        Configuration appsManagerServerConf = new Configuration(config);
        appsManagerServerConf.setClass(YarnConfiguration.YARN_SECURITY_SERVICE_AUTHORIZATION_CLIENT_RESOURCEMANAGER,
                ClientRMSecurityInfo.class, SecurityInfo.class);
        YarnRPC rpc = YarnRPC.create(appsManagerServerConf);
        crmp = ((ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class, remoteAddress, appsManagerServerConf));
    }

    public YarnApplication createApplication(String appName) throws YarnRemoteException {
        return new YarnApplication(this, appName);
    }

    public ClientRMProtocol getClientRMProtocol() {
        return crmp;
    }

    public void killApplication(String appId) throws Exception {
        KillApplicationRequest killRequest = Records.newRecord(KillApplicationRequest.class);
        ApplicationId aid = Records.newRecord(ApplicationId.class);
        long ts = Long.parseLong(appId.substring(appId.indexOf('_') + 1, appId.lastIndexOf('_')));
        aid.setClusterTimestamp(ts);
        int id = Integer.parseInt(appId.substring(appId.lastIndexOf('_') + 1));
        aid.setId(id);
        killRequest.setApplicationId(aid);
        crmp.forceKillApplication(killRequest);
    }
}