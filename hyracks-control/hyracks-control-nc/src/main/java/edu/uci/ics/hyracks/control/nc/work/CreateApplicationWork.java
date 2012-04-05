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
package edu.uci.ics.hyracks.control.nc.work;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.control.common.application.ApplicationStatus;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NodeParameters;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;

public class CreateApplicationWork extends SynchronizableWork {
    private static final Logger LOGGER = Logger.getLogger(CreateApplicationWork.class.getName());

    private final NodeControllerService ncs;

    private final String appName;

    private final boolean deployHar;

    private final byte[] serializedDistributedState;

    public CreateApplicationWork(NodeControllerService ncs, String appName, boolean deployHar,
            byte[] serializedDistributedState) {
        this.ncs = ncs;
        this.appName = appName;
        this.deployHar = deployHar;
        this.serializedDistributedState = serializedDistributedState;
    }

    @Override
    protected void doRun() throws Exception {
        try {
            NCApplicationContext appCtx;
            Map<String, NCApplicationContext> applications = ncs.getApplications();
            if (applications.containsKey(appName)) {
                throw new HyracksException("Duplicate application with name: " + appName + " being created.");
            }
            appCtx = new NCApplicationContext(ncs.getServerContext(), ncs.getRootContext(), appName, ncs.getId());
            applications.put(appName, appCtx);
            if (deployHar) {
                NCConfig ncConfig = ncs.getConfiguration();
                NodeParameters nodeParameters = ncs.getNodeParameters();
                HttpClient hc = new DefaultHttpClient();
                HttpGet get = new HttpGet("http://" + ncConfig.ccHost + ":"
                        + nodeParameters.getClusterControllerInfo().getWebPort() + "/applications/" + appName);
                HttpResponse response = hc.execute(get);
                InputStream is = response.getEntity().getContent();
                OutputStream os = appCtx.getHarOutputStream();
                try {
                    IOUtils.copyLarge(is, os);
                } finally {
                    os.close();
                    is.close();
                }
            }
            appCtx.initializeClassPath();
            appCtx.setDistributedState((Serializable) appCtx.deserialize(serializedDistributedState));
            appCtx.initialize();
            ncs.getClusterController()
                    .notifyApplicationStateChange(ncs.getId(), appName, ApplicationStatus.INITIALIZED);
        } catch (Exception e) {
            LOGGER.warning("Error creating application: " + e.getMessage());
        }
    }
}