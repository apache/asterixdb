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

package org.apache.asterix.experiment.report;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

public abstract class AbstractDynamicDataEvalReportBuilder implements IDynamicDataEvalReportBuilder {

    protected final static String INSTANTANEOUS_INSERT_STRING = "[TimeToInsert100000]";
    protected final static int INSTANTAEOUS_INSERT_COUNT = 100000;
    protected final static int ROUND_COUNT = 721;
    protected final static int ROUND_INTERVAL = 5;
    protected final String expHomePath;
    protected final String expName;
    protected final String runLogFilePath;
    protected final String[] ncLogFilePaths;
    protected BufferedReader br = null;
    protected BufferedReader[] ncLogBrs = null;
    protected final int ncLogFileCount;

    protected final StringBuilder dataGenSb;
    protected final StringBuilder queryGenSb;
    protected final StringBuilder rsb;

    protected AbstractDynamicDataEvalReportBuilder(String expHomePath, String expName, String runLogFileName,
            boolean hasStatFile) {
        this.expHomePath = expHomePath;
        this.expName = expName;
        this.runLogFilePath = new String(expHomePath + runLogFileName);
        if (expName.contains("1A")) {
            ncLogFileCount = 1;
        } else if (expName.contains("1B")) {
            ncLogFileCount = 2;
        } else if (expName.contains("1C")) {
            ncLogFileCount = 4;
        } else /* if (expName.contains("1D") || other exps) */{
            ncLogFileCount = 8;
        }
        ncLogFilePaths = new String[ncLogFileCount];
        ncLogBrs = new BufferedReader[ncLogFileCount];
        for (int i = 0; i < ncLogFileCount; i++) {
            if (hasStatFile) {
                ncLogFilePaths[i] = new String(expHomePath + expName + File.separator + "node" + (i + 1)
                        + File.separator + "logs" + File.separator + "a1_node" + (i + 1) + ".log");
            } else {
                ncLogFilePaths[i] = new String(expHomePath + expName + File.separator + "logs" + File.separator
                        + "a1_node" + (i + 1) + ".log");
            }
        }
        dataGenSb = new StringBuilder();
        queryGenSb = new StringBuilder();
        rsb = new StringBuilder();
    }

    protected void openRunLog() throws IOException {
        br = new BufferedReader(new FileReader(runLogFilePath));
        for (int i = 0; i < ncLogFileCount; i++) {
            ncLogBrs[i] = new BufferedReader(new FileReader(ncLogFilePaths[i]));
        }
    }

    protected void closeRunLog() throws IOException {
        if (br != null) {
            br.close();
        }
        if (ncLogBrs != null) {
            for (int i = 0; i < ncLogFileCount; i++) {
                if (ncLogBrs[i] != null) {
                    ncLogBrs[i].close();
                }
            }
        }
    }

    protected boolean moveToExperimentBegin() throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.contains("Running experiment: " + expName)) {
                return true;
            }
        }
        return false;
    }

    protected void renewStringBuilder() {
        dataGenSb.setLength(0);
        queryGenSb.setLength(0);
        rsb.setLength(0);
    }

    @Override
    public String getInstantaneousInsertPS(int nodeId, boolean useTimeForX) throws Exception {
        renewStringBuilder();
        openRunLog();
        try {

            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }

            int round = 0;
            while (round < ROUND_COUNT) {
                long IIPS = 0;
                String line;
                while ((line = ncLogBrs[nodeId].readLine()) != null) {
                    if (line.contains("IPS")) {
                        IIPS = ReportBuilderHelper.getLong(line, ", IIPS[", "]");
                        break;
                    }
                }
                round++;
                dataGenSb.append(round * ROUND_INTERVAL).append(",").append(IIPS).append("\n");
            }

            return dataGenSb.toString();
        } finally {
            closeRunLog();
        }
    }

    @Override
    public void getAllNodesAccumulatedInsertPS(int targetRound, List<Long> ipsList) throws Exception {
        renewStringBuilder();
        openRunLog();
        ipsList.clear();
        try {

            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return;
            }

            int round = 0;
            while (round < targetRound) {
                long IPSPerRound = 0;
                for (int i = 0; i < ncLogFileCount; i++) {
                    String line;
                    while ((line = ncLogBrs[i].readLine()) != null) {
                        if (line.contains("IPS")) {
                            IPSPerRound += ReportBuilderHelper.getLong(line, ", IPS[", "]");
                            break;
                        }
                    }
                }
                ipsList.add(IPSPerRound);
                round++;
            }
            return;
        } finally {
            closeRunLog();
        }
    }

    public String getInstantaneousDataGenPS(int genId, boolean useTimeForX) throws Exception {
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }

            String line;
            int dGenId;
            int count = 0;
            long timeToInsert = 0;
            long totalTimeToInsert = 0;
            while ((line = br.readLine()) != null) {
                if (line.contains(INSTANTANEOUS_INSERT_STRING)) {
                    dGenId = ReportBuilderHelper.getInt(line, "DataGen[", "]");
                    if (dGenId == genId) {
                        count++;
                        timeToInsert = ReportBuilderHelper.getLong(line, INSTANTANEOUS_INSERT_STRING, "in");
                        totalTimeToInsert += timeToInsert;
                        if (useTimeForX) {
                            dataGenSb.append(totalTimeToInsert / 1000).append(",")
                                    .append(INSTANTAEOUS_INSERT_COUNT / ((double) (timeToInsert) / 1000)).append("\n");
                        } else {
                            dataGenSb.append(count).append(",")
                                    .append(INSTANTAEOUS_INSERT_COUNT / ((double) (timeToInsert) / 1000)).append("\n");
                        }
                    }
                }
                if (line.contains("Running")) {
                    break;
                }
            }
            System.out.println("GenId[" + genId + "] " + totalTimeToInsert + ", " + (totalTimeToInsert / (1000 * 60)));
            return dataGenSb.toString();
        } finally {
            closeRunLog();
        }
    }

    public long getDataGenStartTimeStamp() throws Exception {
        openRunLog();
        try {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains("Running experiment: " + expName)) {
                    while ((line = br.readLine()) != null) {
                        //2015-10-27 17:18:28,242 INFO  [ParallelActionThread 6] transport.TransportImpl (TransportImpl.java:init(155)) - Client identity string: SSH-2.0-SSHJ_0_13_0
                        if (line.contains("INFO  [ParallelActionThread")) {
                            //format1 = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss aa");
                            //format2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                            return ReportBuilderHelper.getTimeStampAsLong(line, format);
                        }
                    }
                }
            }
            return -1;
        } finally {
            closeRunLog();
        }
    }

    public String getIndexSize(String indexDirPath) throws Exception {
        /*
         * exmaple
         * /mnt/data/sdb/youngsk2/asterix/storage/experiments/Tweets_idx_dhbtreeLocation/device_id_0:
        total 211200
        -rw-r--r-- 1 youngsk2 grad 191234048 Jun 29 00:11 2015-06-29-00-09-59-023_2015-06-28-23-51-56-984_b
        -rw-r--r-- 1 youngsk2 grad   7864320 Jun 29 00:11 2015-06-29-00-09-59-023_2015-06-28-23-51-56-984_f
        -rw-r--r-- 1 youngsk2 grad   4194304 Jun 29 00:10 2015-06-29-00-10-26-997_2015-06-29-00-10-26-997_b
        -rw-r--r-- 1 youngsk2 grad    393216 Jun 29 00:10 2015-06-29-00-10-26-997_2015-06-29-00-10-26-997_f
        -rw-r--r-- 1 youngsk2 grad   5898240 Jun 29 00:11 2015-06-29-00-10-59-791_2015-06-29-00-10-59-791_b
        -rw-r--r-- 1 youngsk2 grad    393216 Jun 29 00:11 2015-06-29-00-10-59-791_2015-06-29-00-10-59-791_f
        -rw-r--r-- 1 youngsk2 grad   5898240 Jun 29 00:11 2015-06-29-00-11-30-486_2015-06-29-00-11-30-486_b
        -rw-r--r-- 1 youngsk2 grad    393216 Jun 29 00:11 2015-06-29-00-11-30-486_2015-06-29-00-11-30-486_f
        
         */
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }

            String line;
            String[] tokens;
            long diskSize = 0;
            while ((line = br.readLine()) != null) {
                if (line.contains(indexDirPath)) {
                    br.readLine();//discard "total XXXX" line
                    //read and sum file size
                    while (!(line = br.readLine()).isEmpty()) {
                        tokens = line.split("\\s+");;
                        diskSize += Long.parseLong(tokens[4].trim());
                    }
                }
                if (line.contains("Running")) {
                    break;
                }
            }
            rsb.append((double) diskSize / (1024 * 1024 * 1024));
            return rsb.toString();
        } finally {
            closeRunLog();
        }
    }
}
