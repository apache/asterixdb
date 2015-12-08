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
package org.apache.asterix.api.common;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.asterix.common.config.AsterixPropertiesAccessor;
import org.apache.asterix.common.config.AsterixTransactionProperties;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.hyracks.bootstrap.CCApplicationEntryPoint;
import org.apache.asterix.hyracks.bootstrap.NCApplicationEntryPoint;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;

public class AsterixHyracksIntegrationUtil {

    private static final String IO_DIR_KEY = "java.io.tmpdir";
    public static final int NODES = 2;
    public static final int PARTITONS = 2;

    public static final int DEFAULT_HYRACKS_CC_CLIENT_PORT = 1098;

    public static final int DEFAULT_HYRACKS_CC_CLUSTER_PORT = 1099;

    public static ClusterControllerService cc;
    public static NodeControllerService[] ncs = new NodeControllerService[NODES];
    public static IHyracksClientConnection hcc;

    protected static AsterixTransactionProperties txnProperties;

    public static void init(boolean deleteOldInstanceData) throws Exception {
        AsterixPropertiesAccessor apa = new AsterixPropertiesAccessor();
        txnProperties = new AsterixTransactionProperties(apa);
        if (deleteOldInstanceData) {
            deleteTransactionLogs();
            removeTestStorageFiles();
        }

        CCConfig ccConfig = new CCConfig();
        ccConfig.clusterNetIpAddress = "127.0.0.1";
        ccConfig.clientNetIpAddress = "127.0.0.1";
        ccConfig.clientNetPort = DEFAULT_HYRACKS_CC_CLIENT_PORT;
        ccConfig.clusterNetPort = DEFAULT_HYRACKS_CC_CLUSTER_PORT;
        ccConfig.defaultMaxJobAttempts = 0;
        ccConfig.resultTTL = 30000;
        ccConfig.resultSweepThreshold = 1000;
        ccConfig.appCCMainClass = CCApplicationEntryPoint.class.getName();
        // ccConfig.useJOL = true;
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        // Starts ncs.
        int n = 0;
        for (String ncName : getNcNames()) {
            NCConfig ncConfig1 = new NCConfig();
            ncConfig1.ccHost = "localhost";
            ncConfig1.ccPort = DEFAULT_HYRACKS_CC_CLUSTER_PORT;
            ncConfig1.clusterNetIPAddress = "127.0.0.1";
            ncConfig1.dataIPAddress = "127.0.0.1";
            ncConfig1.resultIPAddress = "127.0.0.1";
            ncConfig1.nodeId = ncName;
            ncConfig1.resultTTL = 30000;
            ncConfig1.resultSweepThreshold = 1000;
            for (int p = 0; p < PARTITONS; ++p) {
                if (p == 0) {
                    ncConfig1.ioDevices = System.getProperty("java.io.tmpdir") + File.separator + ncConfig1.nodeId
                            + "/iodevice" + p;
                } else {
                    ncConfig1.ioDevices += "," + System.getProperty("java.io.tmpdir") + File.separator
                            + ncConfig1.nodeId + "/iodevice" + p;
                }
            }
            ncConfig1.appNCMainClass = NCApplicationEntryPoint.class.getName();
            ncs[n] = new NodeControllerService(ncConfig1);
            ncs[n].start();
            ++n;
        }
        hcc = new HyracksConnection(cc.getConfig().clientNetIpAddress, cc.getConfig().clientNetPort);
    }

    public static String[] getNcNames() {
        String[] names = new String[NODES];
        for (int n = 0; n < NODES; ++n) {
            names[n] = "nc" + (n + 1);
        }
        return names;
    }

    public static String[] getDataDirs() {
        String[] names = new String[NODES];
        for (int n = 0; n < NODES; ++n) {
            names[n] = "nc" + (n + 1) + "data";
        }
        return names;
    }

    public static IHyracksClientConnection getHyracksClientConnection() {
        return hcc;
    }

    public static void deinit(boolean deleteOldInstanceData) throws Exception {
        for (int n = 0; n < ncs.length; ++n) {
            if (ncs[n] != null)
                ncs[n].stop();

        }
        if (cc != null) {
            cc.stop();
        }

        if (deleteOldInstanceData) {
            deleteTransactionLogs();
            removeTestStorageFiles();
        }
    }

    public static void runJob(JobSpecification spec) throws Exception {
        GlobalConfig.ASTERIX_LOGGER.info(spec.toJSON().toString());
        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        GlobalConfig.ASTERIX_LOGGER.info(jobId.toString());
        hcc.waitForCompletion(jobId);
    }

    private static void removeTestStorageFiles() throws IOException {
        File dir = new File(System.getProperty(IO_DIR_KEY));
        for (String ncName : AsterixHyracksIntegrationUtil.getNcNames()) {
            File ncDir = new File(dir, ncName);
            FileUtils.deleteQuietly(ncDir);
        }
    }

    private static void deleteTransactionLogs() throws Exception {
        for (String ncId : AsterixHyracksIntegrationUtil.getNcNames()) {
            File log = new File(txnProperties.getLogDirectory(ncId));
            if (log.exists()) {
                FileUtils.deleteDirectory(log);
            }
        }
    }

    /**
     * main method to run a simple 2 node cluster in-process
     * suggested VM arguments: <code>-enableassertions -Xmx2048m -Dfile.encoding=UTF-8</code>
     *
     * @param args
     *            unused
     */
    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    deinit(false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        try {
            System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, "asterix-build-configuration.xml");

            init(false);
            while (true) {
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
