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
import java.io.FileReader;
import java.io.IOException;

public class ProfilerReportBuilder {

    private static final int INDEX_BUILD_OP_COUNT = 1;
    private static final int PIDX_SCAN_OP_COUNT = 1;
    private static final int WARM_UP_SELECT_QUERY_COUNT = 500;
    private static final int SELECT_QUERY_COUNT = 5000;
    private static final int JOIN_QUERY_COUNT = 200;
    private static final int PARTITION_COUNT = 4;

    private static final int SELECT_QUERY_INVOLVED_INDEX_COUNT = 2;
    private static final int JOIN_QUERY_INVOLVED_INDEX_COUNT = 3;
    private static final int JOIN_RADIUS_TYPE_COUNT = 4;
    private static final int SELECT_RADIUS_TYPE_COUNT = 5;
    private static final int IDX_JOIN_RADIUS_SKIP = JOIN_RADIUS_TYPE_COUNT * JOIN_QUERY_INVOLVED_INDEX_COUNT
            * PARTITION_COUNT - PARTITION_COUNT;
    private static final int IDX_SELECT_RADIUS_SKIP = SELECT_RADIUS_TYPE_COUNT * SELECT_QUERY_INVOLVED_INDEX_COUNT
            * PARTITION_COUNT - PARTITION_COUNT;
    private static final int IDX_INITIAL_JOIN_SKIP = (INDEX_BUILD_OP_COUNT + PIDX_SCAN_OP_COUNT + ((WARM_UP_SELECT_QUERY_COUNT + SELECT_QUERY_COUNT) * SELECT_QUERY_INVOLVED_INDEX_COUNT))
            * PARTITION_COUNT;
    private static final int IDX_INITIAL_SELECT_SKIP = (INDEX_BUILD_OP_COUNT + PIDX_SCAN_OP_COUNT + (WARM_UP_SELECT_QUERY_COUNT * SELECT_QUERY_INVOLVED_INDEX_COUNT))
            * PARTITION_COUNT;
    private static final int FP_JOIN_RADIUS_SKIP = JOIN_RADIUS_TYPE_COUNT * PARTITION_COUNT - PARTITION_COUNT;
    private static final int FP_SELECT_RADIUS_SKIP = SELECT_RADIUS_TYPE_COUNT * PARTITION_COUNT - PARTITION_COUNT;
    private static final int FP_INITIAL_JOIN_SKIP = (PIDX_SCAN_OP_COUNT + WARM_UP_SELECT_QUERY_COUNT + SELECT_QUERY_COUNT)
            * PARTITION_COUNT;
    private static final int FP_INITIAL_SELECT_SKIP = (PIDX_SCAN_OP_COUNT + WARM_UP_SELECT_QUERY_COUNT)
            * PARTITION_COUNT;

    private String indexSearchTimeFilePath = null;
    private String falsePositiveFilePath = null;
    private String cacheMissFilePath = null;
    private BufferedReader brIndexSearchTime;
    private BufferedReader brFalsePositive;
    private BufferedReader brCacheMiss;
    private String line;

    public ProfilerReportBuilder(String indexSearchTimeFilePath, String falsePositiveFilePath, String cacheMissFilePath) {
        this.indexSearchTimeFilePath = indexSearchTimeFilePath;
        this.falsePositiveFilePath = falsePositiveFilePath;
        this.cacheMissFilePath = cacheMissFilePath;
    }

    public String getIdxNumber(boolean getSearchTime, boolean isJoin, int radiusIdx, int indexIdx) throws Exception {
        if (getSearchTime) {
            openIndexSearchTimeFile();
        } else {
            openCacheMissFile();
        }

        StringBuilder sb = new StringBuilder();
        int involvedIndexCount = isJoin ? JOIN_QUERY_INVOLVED_INDEX_COUNT : SELECT_QUERY_INVOLVED_INDEX_COUNT;
        int initialSkip = (isJoin ? IDX_INITIAL_JOIN_SKIP : IDX_INITIAL_SELECT_SKIP) + radiusIdx * involvedIndexCount
                * PARTITION_COUNT + indexIdx * PARTITION_COUNT;
        int radiusSkip = isJoin ? IDX_JOIN_RADIUS_SKIP : IDX_SELECT_RADIUS_SKIP;
        long measuredValue = 0;
        BufferedReader br = getSearchTime ? brIndexSearchTime : brCacheMiss;
        int lineNum = 0;
        int queryCount = isJoin ? JOIN_QUERY_COUNT / JOIN_RADIUS_TYPE_COUNT : SELECT_QUERY_COUNT
                / SELECT_RADIUS_TYPE_COUNT;
        try {

            //initial skip
            for (int i = 0; i < initialSkip; i++) {
                br.readLine();
                ++lineNum;
            }

            for (int j = 0; j < queryCount; j++) {
                //get target index numbers
                for (int i = 0; i < PARTITION_COUNT; i++) {
                    line = br.readLine();
                    measuredValue += Long.parseLong(line);
                    ++lineNum;
                }

                //radius skip
                for (int i = 0; i < radiusSkip; i++) {
                    br.readLine();
                    ++lineNum;
                }
            }

            //System.out.println("lineNum: " + lineNum);
            sb.append((double) measuredValue / (PARTITION_COUNT * queryCount));
            return sb.toString();
        } finally {
            if (getSearchTime) {
                closeIndexSearchTimeFile();
            } else {
                closeCacheMissFile();
            }
        }
    }

    public String getFalsePositives(boolean isJoin, int radiusIdx) throws Exception {
        openFalsePositiveFile();

        StringBuilder sb = new StringBuilder();
        int initialSkip = (isJoin ? FP_INITIAL_JOIN_SKIP : FP_INITIAL_SELECT_SKIP) + radiusIdx * PARTITION_COUNT;
        int radiusSkip = isJoin ? FP_JOIN_RADIUS_SKIP : FP_SELECT_RADIUS_SKIP;
        long falsePositives = 0;
        BufferedReader br = brFalsePositive;
        int lineNum = 0;
        int queryCount = isJoin ? JOIN_QUERY_COUNT / JOIN_RADIUS_TYPE_COUNT : SELECT_QUERY_COUNT
                / SELECT_RADIUS_TYPE_COUNT;
        try {

            //initial skip
            for (int i = 0; i < initialSkip; i++) {
                br.readLine();
                ++lineNum;
            }

            for (int j = 0; j < queryCount; j++) {
                //get target index numbers
                for (int i = 0; i < PARTITION_COUNT; i++) {
                    line = br.readLine();
                    falsePositives += Long.parseLong(line);
                    ++lineNum;
                }

                //radius skip
                for (int i = 0; i < radiusSkip; i++) {
                    br.readLine();
                    ++lineNum;
                }
            }

            //System.out.println("lineNum: " + lineNum);
            sb.append((double) falsePositives / (PARTITION_COUNT * queryCount));
            return sb.toString();
        } finally {
            closeFalsePositiveFile();
        }
    }

    protected void openIndexSearchTimeFile() throws IOException {
        brIndexSearchTime = new BufferedReader(new FileReader(indexSearchTimeFilePath));
    }

    protected void closeIndexSearchTimeFile() throws IOException {
        if (brIndexSearchTime != null) {
            brIndexSearchTime.close();
        }
    }

    protected void openFalsePositiveFile() throws IOException {
        brFalsePositive = new BufferedReader(new FileReader(falsePositiveFilePath));
    }

    protected void closeFalsePositiveFile() throws IOException {
        if (brFalsePositive != null) {
            brFalsePositive.close();
        }
    }

    protected void openCacheMissFile() throws IOException {
        brCacheMiss = new BufferedReader(new FileReader(cacheMissFilePath));
    }

    protected void closeCacheMissFile() throws IOException {
        if (brCacheMiss != null) {
            brCacheMiss.close();
        }
    }

}
