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

public class SIE3ReportBuilder extends AbstractDynamicDataEvalReportBuilder {
    private static final int WARM_UP_QUERY_COUNT = 500;
    private static final int SELECT_QUERY_COUNT = 5000; //5000
    private static final int JOIN_QUERY_COUNT = 1000;
    private static final int SELECT_QUERY_RADIUS_COUNT = 5; //0.00001, 0.0001, 0.001, 0.01, 0.1
    private static final int JOIN_QUERY_RADIUS_COUNT = 4; ////0.00001, 0.0001, 0.001, 0.01
    private static final String LOADED_RECORD_COUNT = "1600000000"; //1600000000
    private final String queryLogFilePath;
    private BufferedReader queryLogFileBr;

    public SIE3ReportBuilder(String expHomePath, String expName, String runLogFileName, String queryLogFileName) {
        super(expHomePath, expName, runLogFileName, false);
        queryLogFilePath = new String(expHomePath + File.separator + expName + File.separator + queryLogFileName);
    }

    private void openQueryLog() throws IOException {
        queryLogFileBr = new BufferedReader(new FileReader(queryLogFilePath));
    }

    private void closeQueryLog() throws IOException {
        if (queryLogFileBr != null) {
            queryLogFileBr.close();
            queryLogFileBr = null;
        }
    }

    @Override
    public String getOverallInsertPS(int minutes) throws Exception {
        return null;
    }

    public String get20minInsertPS(int minutes) throws Exception {
        return null;
    }

    @Override
    public String getInstantaneousQueryPS() throws Exception {
        return null;
    }

    @Override
    public String getQueryPS(int minutes) throws Exception {
        return null;
    }

    public String getIndexCreationTime() throws Exception {
        renewStringBuilder();
        openQueryLog();

        try {
            String line;
            long indexCreationTime = 0;
            while ((line = queryLogFileBr.readLine()) != null) {
                if (line.contains("There is no index with this name")) {
                    indexCreationTime += ReportBuilderHelper.getLong(line, "=", "for");
                    break;
                }
            }
            rsb.append((double) indexCreationTime / (1000 * 60));
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
            int selectQueryCount = 0;
            int targetRadiusSelectQueryCount = 0;
            while ((line = queryLogFileBr.readLine()) != null) {
                if (line.contains(LOADED_RECORD_COUNT)) {
                    //select queries start after total count query
                    // read and discard WARM_UP_QUERY_COUNT queries' results 
                    while (true) {
                        line = queryLogFileBr.readLine();
                        if (line.contains("Elapsed time =")) {
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == WARM_UP_QUERY_COUNT) {
                            break;
                        }
                    }

                    // read and calculate the average query response time for the requested(target) radius
                    while (true) {
                        line = queryLogFileBr.readLine();
                        if (line.contains("Elapsed time =")) {
                            if (selectQueryCount % SELECT_QUERY_RADIUS_COUNT == radiusIdx) {
                                queryResponseTime += ReportBuilderHelper.getLong(line, "=", "for");
                                ++targetRadiusSelectQueryCount;
                            }
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == WARM_UP_QUERY_COUNT + SELECT_QUERY_COUNT) {
                            break;
                        }
                    }
                    break;
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
            int selectQueryCount = 0;
            int targetRadiusSelectQueryCount = 0;
            while ((line = queryLogFileBr.readLine()) != null) {
                if (line.contains(LOADED_RECORD_COUNT)) {
                    //select queries start after total count query
                    // read and discard WARM_UP_QUERY_COUNT queries' results 
                    while (true) {
                        line = queryLogFileBr.readLine();
                        if (line.contains("int64")) {
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == WARM_UP_QUERY_COUNT) {
                            break;
                        }
                    }

                    // read and calculate the average query response time for the requested(target) radius
                    while (true) {
                        line = queryLogFileBr.readLine();
                        if (line.contains("int64")) {
                            if (selectQueryCount % SELECT_QUERY_RADIUS_COUNT == radiusIdx) {
                                line = queryLogFileBr.readLine();
                                queryResultCount += Long.parseLong(line);
                                ++targetRadiusSelectQueryCount;
                            }
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == WARM_UP_QUERY_COUNT + SELECT_QUERY_COUNT) {
                            break;
                        }
                    }
                    break;
                }
            }
            rsb.append((double) queryResultCount / targetRadiusSelectQueryCount);
            return rsb.toString();
        } finally {
            closeQueryLog();
        }
    }

    public String getJoinQueryResponseTime(int radiusIdx) throws Exception {
        renewStringBuilder();
        openQueryLog();
        try {

            String line;
            long queryResponseTime = 0;
            int selectQueryCount = 0;
            int targetRadiusJoinQueryCount = 0;
            while ((line = queryLogFileBr.readLine()) != null) {
                if (line.contains(LOADED_RECORD_COUNT)) {
                    //select queries start after total count query
                    // read and discard WARM_UP_QUERY_COUNT + SELECT_QUERY_COUNT queries' results 
                    while (true) {
                        line = queryLogFileBr.readLine();
                        if (line.contains("Elapsed time =")) {
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == WARM_UP_QUERY_COUNT + SELECT_QUERY_COUNT) {
                            break;
                        }
                    }

                    selectQueryCount = 0;
                    // read and calculate the average query response time for the requested(target) radius
                    while (true) {
                        line = queryLogFileBr.readLine();
                        if (line.contains("Elapsed time =")) {
                            if (selectQueryCount % JOIN_QUERY_RADIUS_COUNT == radiusIdx) {
                                if (ReportBuilderHelper.getLong(line, "=", "for") > 5000) {
                                    System.out.println("Time: " + expName + "[" + radiusIdx + ", "
                                            + targetRadiusJoinQueryCount + ", " + selectQueryCount + "]:"
                                            + ReportBuilderHelper.getLong(line, "=", "for"));
                                }
                                queryResponseTime += ReportBuilderHelper.getLong(line, "=", "for");
                                ++targetRadiusJoinQueryCount;
                            }
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == JOIN_QUERY_COUNT) {
                            break;
                        }
                    }
                    break;
                }
            }
            rsb.append((double) queryResponseTime / targetRadiusJoinQueryCount);
            return rsb.toString();
        } finally {
            closeQueryLog();
        }
    }

    public String getJoinQueryResultCount(int radiusIdx) throws Exception {
        renewStringBuilder();
        openQueryLog();
        try {

            String line;
            long queryResultCount = 0;
            int selectQueryCount = 0;
            int targetRadiusJoinQueryCount = 0;
            while ((line = queryLogFileBr.readLine()) != null) {
                if (line.contains(LOADED_RECORD_COUNT)) {
                    //select queries start after total count query
                    // read and discard WARM_UP_QUERY_COUNT + SELECT_QUERY_COUNT queries' results 
                    while (true) {
                        line = queryLogFileBr.readLine();
                        if (line.contains("int64")) {
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == WARM_UP_QUERY_COUNT + SELECT_QUERY_COUNT) {
                            break;
                        }
                    }

                    selectQueryCount = 0;
                    // read and calculate the average query response time for the requested(target) radius
                    while (true) {
                        line = queryLogFileBr.readLine();
                        if (line.contains("int64")) {
                            if (selectQueryCount % JOIN_QUERY_RADIUS_COUNT == radiusIdx) {
                                line = queryLogFileBr.readLine();

                                if (selectQueryCount == 600 || selectQueryCount == 824 || Long.parseLong(line) > 100000) {
                                    System.out.println("Count: " + expName + "[" + radiusIdx + ", "
                                            + targetRadiusJoinQueryCount + ", " + selectQueryCount + "]:"
                                            + Long.parseLong(line));
                                }

                                queryResultCount += Long.parseLong(line);
                                ++targetRadiusJoinQueryCount;
                            }
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == JOIN_QUERY_COUNT) {
                            break;
                        }
                    }
                    break;
                }
            }
            rsb.append((double) queryResultCount / targetRadiusJoinQueryCount);
            return rsb.toString();
        } finally {
            closeQueryLog();
        }
    }
}
