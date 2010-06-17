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
package edu.uci.ics.hyracks.tests.integration;

import java.util.EnumSet;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.config.CCConfig;
import edu.uci.ics.hyracks.config.NCConfig;
import edu.uci.ics.hyracks.controller.ClusterControllerService;
import edu.uci.ics.hyracks.controller.NodeControllerService;

public abstract class AbstractIntegrationTest {
    public static final String NC1_ID = "nc1";
    public static final String NC2_ID = "nc2";

    protected static ClusterControllerService cc;
    protected static NodeControllerService nc1;
    protected static NodeControllerService nc2;

    @BeforeClass
    public static void init() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.port = 39001;
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        NCConfig ncConfig1 = new NCConfig();
        ncConfig1.ccHost = "localhost";
        ncConfig1.ccPort = 39001;
        ncConfig1.dataIPAddress = "127.0.0.1";
        ncConfig1.nodeId = NC1_ID;
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();

        NCConfig ncConfig2 = new NCConfig();
        ncConfig2.ccHost = "localhost";
        ncConfig2.ccPort = 39001;
        ncConfig2.dataIPAddress = "127.0.0.1";
        ncConfig2.nodeId = NC2_ID;
        nc2 = new NodeControllerService(ncConfig2);
        nc2.start();
    }

    @AfterClass
    public static void deinit() throws Exception {
        nc2.stop();
        nc1.stop();
        cc.stop();
    }

    void runTest(JobSpecification spec) throws Exception {
        UUID jobId = cc.createJob(spec, EnumSet.of(JobFlag.COLLECT_FRAME_COUNTS));
        System.err.println(spec.toJSON());
        cc.start(jobId);
        System.err.print(jobId);
        System.err.println(cc.waitForCompletion(jobId));
    }
}