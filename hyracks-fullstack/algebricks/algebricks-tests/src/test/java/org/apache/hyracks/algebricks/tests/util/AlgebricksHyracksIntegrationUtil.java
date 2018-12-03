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
package org.apache.hyracks.algebricks.tests.util;

import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.File;
import java.util.EnumSet;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.ipc.impl.HyracksConnection;

public class AlgebricksHyracksIntegrationUtil {

    public static final String NC1_ID = "nc1";
    public static final String NC2_ID = "nc2";

    public static final int TEST_HYRACKS_CC_CLUSTER_NET_PORT = 4322;
    public static final int TEST_HYRACKS_CC_CLIENT_NET_PORT = 4321;

    private static ClusterControllerService cc;
    public static NodeControllerService nc1;
    public static NodeControllerService nc2;
    private static IHyracksClientConnection hcc;

    public static void init() throws Exception {
        FileUtils.deleteQuietly(new File(joinPath("target", "data")));
        FileUtils.copyDirectory(new File("data"), new File(joinPath("target", "data")));
        CCConfig ccConfig = new CCConfig();
        ccConfig.setClientListenAddress("127.0.0.1");
        ccConfig.setClientListenPort(TEST_HYRACKS_CC_CLIENT_NET_PORT);
        ccConfig.setClusterListenAddress("127.0.0.1");
        ccConfig.setClusterListenPort(TEST_HYRACKS_CC_CLUSTER_NET_PORT);
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        NCConfig ncConfig1 = new NCConfig(NC1_ID);
        ncConfig1.setClusterAddress("localhost");
        ncConfig1.setClusterPort(TEST_HYRACKS_CC_CLUSTER_NET_PORT);
        ncConfig1.setClusterListenAddress("127.0.0.1");
        ncConfig1.setDataListenAddress("127.0.0.1");
        ncConfig1.setResultListenAddress("127.0.0.1");
        ncConfig1.setIODevices(new String[] { joinPath(System.getProperty("user.dir"), "target", "data", "device0") });
        FileUtils.forceMkdir(new File(ncConfig1.getIODevices()[0]));
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();

        NCConfig ncConfig2 = new NCConfig(NC2_ID);
        ncConfig2.setClusterAddress("localhost");
        ncConfig2.setClusterPort(TEST_HYRACKS_CC_CLUSTER_NET_PORT);
        ncConfig2.setClusterListenAddress("127.0.0.1");
        ncConfig2.setDataListenAddress("127.0.0.1");
        ncConfig2.setResultListenAddress("127.0.0.1");
        ncConfig2.setIODevices(new String[] { joinPath(System.getProperty("user.dir"), "target", "data", "device1") });
        FileUtils.forceMkdir(new File(ncConfig1.getIODevices()[0]));
        nc2 = new NodeControllerService(ncConfig2);
        nc2.start();

        hcc = new HyracksConnection(ccConfig.getClientListenAddress(), ccConfig.getClientListenPort());
    }

    public static void deinit() throws Exception {
        nc2.stop();
        nc1.stop();
        cc.stop();
    }

    public static void runJob(JobSpecification spec) throws Exception {
        boolean loggerInfoEnabled = AlgebricksConfig.ALGEBRICKS_LOGGER.isInfoEnabled();
        if (loggerInfoEnabled) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.info(spec.toJSON().toString());
        }
        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        if (loggerInfoEnabled) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.info(jobId.toString());
        }
        hcc.waitForCompletion(jobId);
    }
}
