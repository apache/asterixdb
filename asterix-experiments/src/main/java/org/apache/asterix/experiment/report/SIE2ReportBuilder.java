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

public class SIE2ReportBuilder extends AbstractDynamicDataEvalReportBuilder {
    private static final int SELECT_QUERY_RADIUS_COUNT = 5;
    private static final int INITIAL_SELECT_QUERY_COUNT_TO_IGNORE = 0;
    private static final int MAX_SELECT_QUERY_COUNT_TO_CONSIDER = Integer.MAX_VALUE; //5000 + INITIAL_SELECT_QUERY_COUNT_TO_CONSIDER;
    private static final int QUERY_GEN_COUNT = 8;
    private static final String[] QUERY_GEN_IPS = { "130.149.249.61", "130.149.249.62", "130.149.249.63",
            "130.149.249.64", "130.149.249.65", "130.149.249.66", "130.149.249.67", "130.149.249.68" };
    private BufferedReader[] queryLogFileBrs;

    public SIE2ReportBuilder(String expHomePath, String expName, String runLogFileName) {
        super(expHomePath, expName, runLogFileName, false);
        queryLogFileBrs = new BufferedReader[QUERY_GEN_COUNT];
    }

    @Override
    public String getOverallInsertPS(int minutes) throws Exception {
        return null;
    }

    public String get20minInsertPS(int minutes) throws Exception {
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }

            String line;
            long insertCount = 0;
            while ((line = br.readLine()) != null) {
                if (line.contains("[During ingestion + queries][InsertCount]")) {
                    insertCount += ReportBuilderHelper.getLong(line, "=", "in");
                }
                if (line.contains("Running")) {
                    break;
                }
            }
            rsb.append(insertCount / (minutes * 60));
            return rsb.toString();
        } finally {
            closeRunLog();
        }
    }

    public double getFirstXminInsertPS(int minutes, int genId, int unitMinutes) throws Exception {
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return 0;
            }

            String line;
            int dGenId;
            int count = 0;
            long timeToInsert = 0;
            long totalTimeToInsert = 0;
            boolean haveResult = false;
            while ((line = br.readLine()) != null) {
                if (line.contains("[During ingestion only][TimeToInsert100000]")) {
                    dGenId = ReportBuilderHelper.getInt(line, "DataGen[", "]");
                    if (dGenId == genId) {
                        count++;
                        timeToInsert = ReportBuilderHelper.getLong(line, INSTANTANEOUS_INSERT_STRING, "in");
                        totalTimeToInsert += timeToInsert;
                        if (totalTimeToInsert > minutes * 60000) {
                            haveResult = true;
                            break;
                        }
                    }
                }
                if (line.contains("Running")) {
                    break;
                }
            }
            if (haveResult || totalTimeToInsert > (minutes * 60000 - unitMinutes * 60000)) {
                return (count * INSTANTAEOUS_INSERT_COUNT) / ((double) totalTimeToInsert / 1000);
            } else {
                return 0;
                //return  ((count * INSTANTAEOUS_INSERT_COUNT) / ((double)totalTimeToInsert/1000)) * -1;
            }
        } finally {
            closeRunLog();
        }
    }

    @Override
    public String getInstantaneousQueryPS() throws Exception {
        return null;
    }

    @Override
    public String getQueryPS(int minutes) throws Exception {
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }

            String line;
            long queryCount = 0;
            while ((line = br.readLine()) != null) {
                if (line.contains("[QueryCount]")) {
                    queryCount += ReportBuilderHelper.getLong(line, "[QueryCount]", "in");
                }
                if (line.contains("Running")) {
                    break;
                }
            }
            rsb.append(queryCount / (float) (minutes * 60));
            return rsb.toString();
        } finally {
            closeRunLog();
        }
    }

    public String getAverageQueryResultCount() throws Exception {
        renewStringBuilder();
        openQueryLog();
        try {
            String line;
            long resultCount = 0;
            long queryCount = 0;
            for (BufferedReader queryLogFileBr : queryLogFileBrs) {
                while ((line = queryLogFileBr.readLine()) != null) {
                    if (line.contains("int64")) {
                        line = queryLogFileBr.readLine();
                        resultCount += Long.parseLong(line);
                        ++queryCount;
                    }
                }
            }
            rsb.append(resultCount / queryCount);
            return rsb.toString();
        } finally {
            closeQueryLog();
        }
    }

    public String getAverageQueryResponseTime() throws Exception {
        renewStringBuilder();
        openQueryLog();
        try {
            String line;
            long responseTime = 0;
            long queryCount = 0;
            for (BufferedReader queryLogFileBr : queryLogFileBrs) {
                while ((line = queryLogFileBr.readLine()) != null) {
                    if (line.contains("Elapsed time = ")) {
                        responseTime += ReportBuilderHelper.getLong(line, "=", "for");
                        ++queryCount;
                    }
                }
            }
            rsb.append(responseTime / queryCount);
            return rsb.toString();
        } finally {
            closeQueryLog();
        }
    }

    public String getSelectQueryResponseTime(int radiusIdx) throws Exception {
        renewStringBuilder();
        openQueryLog();
        try {
            String line;
            long queryResponseTime = 0;
            int targetRadiusSelectQueryCount = 0;
            for (BufferedReader queryLogFileBr : queryLogFileBrs) {
                int selectQueryCount = 0;
                while ((line = queryLogFileBr.readLine()) != null) {
                    if (line.contains("Elapsed time =") && selectQueryCount < MAX_SELECT_QUERY_COUNT_TO_CONSIDER) {
                        if (selectQueryCount % SELECT_QUERY_RADIUS_COUNT == radiusIdx
                                && selectQueryCount >= INITIAL_SELECT_QUERY_COUNT_TO_IGNORE) {
                            queryResponseTime += ReportBuilderHelper.getLong(line, "=", "for");
                            ++targetRadiusSelectQueryCount;
                        }
                        ++selectQueryCount;
                    }
                }
            }
            rsb.append((double) queryResponseTime / targetRadiusSelectQueryCount);
            return rsb.toString();
        } finally {
            closeQueryLog();
        }
    }

    public String getSelectQueryResultCount(int radiusIdx) throws Exception {
        renewStringBuilder();
        openQueryLog();
        try {
            String line;
            long queryResultCount = 0;
            int targetRadiusSelectQueryCount = 0;
            for (BufferedReader queryLogFileBr : queryLogFileBrs) {
                int selectQueryCount = 0;
                while ((line = queryLogFileBr.readLine()) != null) {
                    if (line.contains("int64") && selectQueryCount < MAX_SELECT_QUERY_COUNT_TO_CONSIDER) {
                        if (selectQueryCount % SELECT_QUERY_RADIUS_COUNT == radiusIdx
                                && selectQueryCount >= INITIAL_SELECT_QUERY_COUNT_TO_IGNORE) {
                            line = queryLogFileBr.readLine(); //read the result count line
                            queryResultCount += Long.parseLong(line);
                            ++targetRadiusSelectQueryCount;
                        }
                        ++selectQueryCount;
                    }
                }
            }
            rsb.append((double) queryResultCount / targetRadiusSelectQueryCount);
            return rsb.toString();
        } finally {
            closeQueryLog();
        }
    }

    private void openQueryLog() throws IOException {
        String queryLogFilePathPrefix = expHomePath + File.separator + expName + File.separator + "QueryGenResult-";
        String queryLogFilePathSuffix = ".txt";
        for (int i = 0; i < QUERY_GEN_COUNT; i++) {
            queryLogFileBrs[i] = new BufferedReader(new FileReader(queryLogFilePathPrefix + QUERY_GEN_IPS[i]
                    + queryLogFilePathSuffix));
        }
    }

    private void closeQueryLog() throws IOException {
        for (BufferedReader queryLogFileBr : queryLogFileBrs) {
            if (queryLogFileBr != null) {
                queryLogFileBr.close();
                queryLogFileBr = null;
            }
        }
    }
}
