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

package org.apache.hyracks.hdfs.utils;

import java.util.EnumSet;

import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.ipc.impl.HyracksConnection;

public class HyracksUtils {

    public static final String NC1_ID = "nc1";
    public static final String NC2_ID = "nc2";

    public static final int DEFAULT_HYRACKS_CC_PORT = 1099;
    public static final int TEST_HYRACKS_CC_PORT = 1099;
    public static final int TEST_HYRACKS_CC_CLIENT_PORT = 2099;
    public static final String CC_HOST = "localhost";

    public static final int FRAME_SIZE = 65536;

    private static ClusterControllerService cc;
    private static NodeControllerService nc1;
    private static NodeControllerService nc2;
    private static IHyracksClientConnection hcc;

    public static void init() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.setClientListenAddress(CC_HOST);
        ccConfig.setClusterListenAddress(CC_HOST);
        ccConfig.setClusterListenPort(TEST_HYRACKS_CC_PORT);
        ccConfig.setClientListenPort(TEST_HYRACKS_CC_CLIENT_PORT);
        ccConfig.setJobHistorySize(0);
        ccConfig.setProfileDumpPeriod(-1);

        // cluster controller
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        // two node controllers
        NCConfig ncConfig1 = new NCConfig(NC1_ID);
        ncConfig1.setClusterAddress("localhost");
        ncConfig1.setClusterListenAddress("localhost");
        ncConfig1.setClusterPort(TEST_HYRACKS_CC_PORT);
        ncConfig1.setDataListenAddress("127.0.0.1");
        ncConfig1.setResultListenAddress("127.0.0.1");
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();

        NCConfig ncConfig2 = new NCConfig(NC2_ID);
        ncConfig2.setClusterAddress("localhost");
        ncConfig2.setClusterListenAddress("localhost");
        ncConfig2.setClusterPort(TEST_HYRACKS_CC_PORT);
        ncConfig2.setDataListenAddress("127.0.0.1");
        ncConfig2.setResultListenAddress("127.0.0.1");
        nc2 = new NodeControllerService(ncConfig2);
        nc2.start();

        // hyracks connection
        hcc = new HyracksConnection(CC_HOST, TEST_HYRACKS_CC_CLIENT_PORT);
    }

    public static void deinit() throws Exception {
        nc2.stop();
        nc1.stop();
        cc.stop();
    }

    public static void runJob(JobSpecification spec, String appName) throws Exception {
        spec.setFrameSize(FRAME_SIZE);
        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        hcc.waitForCompletion(jobId);
    }

}
