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
package org.apache.asterix.event.error;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.event.model.AsterixInstance;
import org.apache.asterix.event.model.AsterixInstance.State;
import org.apache.asterix.event.model.AsterixRuntimeState;
import org.apache.asterix.event.model.ProcessInfo;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.event.service.AsterixEventService;
import org.apache.asterix.event.service.AsterixEventServiceUtil;

public class VerificationUtil {

    private static final String VERIFY_SCRIPT_PATH = AsterixEventService.getEventHome() + File.separator + "scripts"
            + File.separator + "verify.sh";

    public static AsterixRuntimeState getAsterixRuntimeState(AsterixInstance instance) throws Exception {

        Cluster cluster = instance.getCluster();
        List<String> args = new ArrayList<String>();
        args.add(instance.getName());
        args.add(instance.getCluster().getMasterNode().getClusterIp());
        for (Node node : cluster.getNode()) {
            args.add(node.getClusterIp());
            args.add(instance.getName() + "_" + node.getId());
        }
        Thread.sleep(2000);
        String output = AsterixEventServiceUtil.executeLocalScript(VERIFY_SCRIPT_PATH, args);
        boolean ccRunning = true;
        List<String> failedNCs = new ArrayList<String>();
        String[] infoFields;
        ProcessInfo pInfo;
        List<ProcessInfo> processes = new ArrayList<ProcessInfo>();

        for (String line : output.split("\n")) {
            String nodeid = null;
            infoFields = line.split(":");
            try {
                int pid = Integer.parseInt(infoFields[3]);
                if (infoFields[0].equals("NC")) {
                    nodeid = infoFields[2].split("_")[1];
                } else {
                    nodeid = instance.getCluster().getMasterNode().getId();
                }
                pInfo = new ProcessInfo(infoFields[0], infoFields[1], nodeid, pid);
                processes.add(pInfo);
            } catch (Exception e) {
                if (infoFields[0].equalsIgnoreCase("CC")) {
                    ccRunning = false;
                } else {
                    failedNCs.add(infoFields[1]);
                }
            }
        }
        return new AsterixRuntimeState(processes, failedNCs, ccRunning);
    }

    public static void updateInstanceWithRuntimeDescription(AsterixInstance instance, AsterixRuntimeState state,
            boolean expectedRunning) {
        StringBuffer summary = new StringBuffer();
        if (expectedRunning) {
            if (!state.isCcRunning()) {
                summary.append("Cluster Controller not running at " + instance.getCluster().getMasterNode().getId()
                        + "\n");
                instance.setState(State.UNUSABLE);
            }
            if (state.getFailedNCs() != null && !state.getFailedNCs().isEmpty()) {
                summary.append("Node Controller not running at the following nodes" + "\n");
                for (String failedNC : state.getFailedNCs()) {
                    summary.append(failedNC + "\n");
                }
                // instance.setState(State.UNUSABLE);
            }
            if (!(instance.getState().equals(State.UNUSABLE))) {
                instance.setState(State.ACTIVE);
            }
        } else {
            if (state.getProcesses() != null && state.getProcesses().size() > 0) {
                summary.append("Following process still running " + "\n");
                for (ProcessInfo pInfo : state.getProcesses()) {
                    summary.append(pInfo + "\n");
                }
                // instance.setState(State.UNUSABLE);
            } else {
                // instance.setState(State.INACTIVE);
            }
        }
        state.setSummary(summary.toString());
        instance.setAsterixRuntimeStates(state);
    }

    public static void verifyBackupRestoreConfiguration(String hdfsUrl, String hadoopVersion, String hdfsBackupDir)
            throws Exception {
        StringBuffer errorCheck = new StringBuffer();
        if (hdfsUrl == null || hdfsUrl.length() == 0) {
            errorCheck.append("\n HDFS Url not configured");
        }
        if (hadoopVersion == null || hadoopVersion.length() == 0) {
            errorCheck.append("\n HDFS version not configured");
        }
        if (hdfsBackupDir == null || hdfsBackupDir.length() == 0) {
            errorCheck.append("\n HDFS backup directory not configured");
        }
        if (errorCheck.length() > 0) {
            throw new Exception("Incomplete hdfs configuration" + errorCheck);
        }
    }
}
