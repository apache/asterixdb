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
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

public class SIE3ReportBuilderRunner {
    static boolean IS_PROFILE = false;
    String outputFilePath = "/Users/kisskys/workspace/asterix_master/resultLog/MemBuf3g-DiskBuf3g-Lsev-Jvm7g-Lock0g/result-report/";
    SIE3ReportBuilder sie3Dhbtree;
    SIE3ReportBuilder sie3Dhvbtree;
    SIE3ReportBuilder sie3Rtree;
    SIE3ReportBuilder sie3Shbtree;
    SIE3ReportBuilder sie3Sif;

    //for profiling report -------------------------------------
    String profileFileHomeDir;
    String indexSearchTimeFilePath;
    String falsePositiveFilePath;
    String cacheMissFilePath;
    ProfilerReportBuilder dhbtreeProfiler;
    ProfilerReportBuilder dhvbtreeProfiler;
    ProfilerReportBuilder rtreeProfiler;
    ProfilerReportBuilder shbtreeProfiler;
    ProfilerReportBuilder sifProfiler;
    //for profiling report -------------------------------------

    StringBuilder sb = new StringBuilder();

    public SIE3ReportBuilderRunner() {

        String expHomePath = "/Users/kisskys/workspace/asterix_master/resultLog/MemBuf3g-DiskBuf3g-Lsev-Jvm7g-Lock0g/exp3/";
        String runLogFileName = "run-exp3.log";
        String queryLogFileNamePrefix = "QueryGenResult-";
        String queryLogFileNameSuffix = "-130.149.249.51.txt";

        sie3Dhbtree = new SIE3ReportBuilder(expHomePath, "SpatialIndexExperiment3Dhbtree", runLogFileName,
                queryLogFileNamePrefix + "SpatialIndexExperiment3Dhbtree" + queryLogFileNameSuffix);
        sie3Dhvbtree = new SIE3ReportBuilder(expHomePath, "SpatialIndexExperiment3Dhvbtree", runLogFileName,
                queryLogFileNamePrefix + "SpatialIndexExperiment3Dhvbtree" + queryLogFileNameSuffix);
        sie3Rtree = new SIE3ReportBuilder(expHomePath, "SpatialIndexExperiment3Rtree", runLogFileName,
                queryLogFileNamePrefix + "SpatialIndexExperiment3Rtree" + queryLogFileNameSuffix);
        sie3Shbtree = new SIE3ReportBuilder(expHomePath, "SpatialIndexExperiment3Shbtree", runLogFileName,
                queryLogFileNamePrefix + "SpatialIndexExperiment3Shbtree" + queryLogFileNameSuffix);
        sie3Sif = new SIE3ReportBuilder(expHomePath, "SpatialIndexExperiment3Sif", runLogFileName,
                queryLogFileNamePrefix + "SpatialIndexExperiment3Sif" + queryLogFileNameSuffix);
    }

    public void generateIndexCreationTime() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 index creation time report\n");
        sb.append("index type, index creation time\n");
        sb.append("dhbtree,").append(sie3Dhbtree.getIndexCreationTime()).append("\n");
        sb.append("dhvbtree,").append(sie3Dhvbtree.getIndexCreationTime()).append("\n");
        sb.append("rtree,").append(sie3Rtree.getIndexCreationTime()).append("\n");
        sb.append("shbtree,").append(sie3Shbtree.getIndexCreationTime()).append("\n");
        sb.append("sif,").append(sie3Sif.getIndexCreationTime()).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie3_index_creation_time.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateIndexSize() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 index size report\n");

        sb.append("index type, index size\n");
        sb.append("dhbtree,").append(sie3Dhbtree.getIndexSize("Tweets_idx_dhbtreeLocation/device_id")).append("\n");
        sb.append("dhvbtree,").append(sie3Dhvbtree.getIndexSize("Tweets_idx_dhvbtreeLocation/device_id")).append("\n");
        sb.append("rtree,").append(sie3Rtree.getIndexSize("Tweets_idx_rtreeLocation/device_id")).append("\n");
        sb.append("shbtree,").append(sie3Shbtree.getIndexSize("Tweets_idx_shbtreeLocation/device_id")).append("\n");
        sb.append("sif,").append(sie3Sif.getIndexSize("Tweets_idx_sifLocation/device_id")).append("\n");
        sb.append("# pidx,").append(sie3Sif.getIndexSize("Tweets_idx_Tweets/device_id")).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie3_sidx_size.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateSelectQueryResponseTime() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 select query response time report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(sie3Dhbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie3Dhvbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie3Rtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie3Shbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie3Sif.getSelectQueryResponseTime(0)).append("\n");
        sb.append("0.0001,").append(sie3Dhbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie3Dhvbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie3Rtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie3Shbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie3Sif.getSelectQueryResponseTime(1)).append("\n");
        sb.append("0.001,").append(sie3Dhbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie3Dhvbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie3Rtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie3Shbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie3Sif.getSelectQueryResponseTime(2)).append("\n");
        sb.append("0.01,").append(sie3Dhbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie3Dhvbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie3Rtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie3Shbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie3Sif.getSelectQueryResponseTime(3)).append("\n");
        sb.append("0.1,").append(sie3Dhbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie3Dhvbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie3Rtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie3Shbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie3Sif.getSelectQueryResponseTime(4)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie3_select_query_response_time.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateSelectQueryResultCount() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 select query result count report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(sie3Dhbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie3Dhvbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie3Rtree.getSelectQueryResultCount(0)).append(",")
                .append(sie3Shbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie3Sif.getSelectQueryResultCount(0)).append("\n");
        sb.append("0.0001,").append(sie3Dhbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie3Dhvbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie3Rtree.getSelectQueryResultCount(1)).append(",")
                .append(sie3Shbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie3Sif.getSelectQueryResultCount(1)).append("\n");
        sb.append("0.001,").append(sie3Dhbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie3Dhvbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie3Rtree.getSelectQueryResultCount(2)).append(",")
                .append(sie3Shbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie3Sif.getSelectQueryResultCount(2)).append("\n");
        sb.append("0.01,").append(sie3Dhbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie3Dhvbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie3Rtree.getSelectQueryResultCount(3)).append(",")
                .append(sie3Shbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie3Sif.getSelectQueryResultCount(3)).append("\n");
        sb.append("0.1,").append(sie3Dhbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie3Dhvbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie3Rtree.getSelectQueryResultCount(4)).append(",")
                .append(sie3Shbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie3Sif.getSelectQueryResultCount(4)).append("\n");

        FileOutputStream fos = ReportBuilderHelper
                .openOutputFile(outputFilePath + "sie3_select_query_result_count.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateJoinQueryResponseTime() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 join query response time report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(sie3Dhbtree.getJoinQueryResponseTime(0)).append(",")
                .append(sie3Dhvbtree.getJoinQueryResponseTime(0)).append(",")
                .append(sie3Rtree.getJoinQueryResponseTime(0)).append(",")
                .append(sie3Shbtree.getJoinQueryResponseTime(0)).append(",")
                .append(sie3Sif.getJoinQueryResponseTime(0)).append("\n");
        sb.append("0.0001,").append(sie3Dhbtree.getJoinQueryResponseTime(1)).append(",")
                .append(sie3Dhvbtree.getJoinQueryResponseTime(1)).append(",")
                .append(sie3Rtree.getJoinQueryResponseTime(1)).append(",")
                .append(sie3Shbtree.getJoinQueryResponseTime(1)).append(",")
                .append(sie3Sif.getJoinQueryResponseTime(1)).append("\n");
        sb.append("0.001,").append(sie3Dhbtree.getJoinQueryResponseTime(2)).append(",")
                .append(sie3Dhvbtree.getJoinQueryResponseTime(2)).append(",")
                .append(sie3Rtree.getJoinQueryResponseTime(2)).append(",")
                .append(sie3Shbtree.getJoinQueryResponseTime(2)).append(",")
                .append(sie3Sif.getJoinQueryResponseTime(2)).append("\n");
        sb.append("0.01,").append(sie3Dhbtree.getJoinQueryResponseTime(3)).append(",")
                .append(sie3Dhvbtree.getJoinQueryResponseTime(3)).append(",")
                .append(sie3Rtree.getJoinQueryResponseTime(3)).append(",")
                .append(sie3Shbtree.getJoinQueryResponseTime(3)).append(",")
                .append(sie3Sif.getJoinQueryResponseTime(3)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie3_join_query_response_time.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateJoinQueryResultCount() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 join query result count report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(sie3Dhbtree.getJoinQueryResultCount(0)).append(",")
                .append(sie3Dhvbtree.getJoinQueryResultCount(0)).append(",")
                .append(sie3Rtree.getJoinQueryResultCount(0)).append(",")
                .append(sie3Shbtree.getJoinQueryResultCount(0)).append(",").append(sie3Sif.getJoinQueryResultCount(0))
                .append("\n");
        sb.append("0.0001,").append(sie3Dhbtree.getJoinQueryResultCount(1)).append(",")
                .append(sie3Dhvbtree.getJoinQueryResultCount(1)).append(",")
                .append(sie3Rtree.getJoinQueryResultCount(1)).append(",")
                .append(sie3Shbtree.getJoinQueryResultCount(1)).append(",").append(sie3Sif.getJoinQueryResultCount(1))
                .append("\n");
        sb.append("0.001,").append(sie3Dhbtree.getJoinQueryResultCount(2)).append(",")
                .append(sie3Dhvbtree.getJoinQueryResultCount(2)).append(",")
                .append(sie3Rtree.getJoinQueryResultCount(2)).append(",")
                .append(sie3Shbtree.getJoinQueryResultCount(2)).append(",").append(sie3Sif.getJoinQueryResultCount(2))
                .append("\n");
        sb.append("0.01,").append(sie3Dhbtree.getJoinQueryResultCount(3)).append(",")
                .append(sie3Dhvbtree.getJoinQueryResultCount(3)).append(",")
                .append(sie3Rtree.getJoinQueryResultCount(3)).append(",")
                .append(sie3Shbtree.getJoinQueryResultCount(3)).append(",").append(sie3Sif.getJoinQueryResultCount(3))
                .append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie3_join_query_result_count.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateSelectQueryProfiledSidxSearchTime() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 select query profiled sidx search time report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(dhbtreeProfiler.getIdxNumber(true, false, 0, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, false, 0, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, false, 0, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, false, 0, 0)).append(",")
                .append(sifProfiler.getIdxNumber(true, false, 0, 0)).append("\n");
        sb.append("0.0001,").append(dhbtreeProfiler.getIdxNumber(true, false, 1, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, false, 1, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, false, 1, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, false, 1, 0)).append(",")
                .append(sifProfiler.getIdxNumber(true, false, 1, 0)).append("\n");
        sb.append("0.001,").append(dhbtreeProfiler.getIdxNumber(true, false, 2, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, false, 2, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, false, 2, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, false, 2, 0)).append(",")
                .append(sifProfiler.getIdxNumber(true, false, 2, 0)).append("\n");
        sb.append("0.01,").append(dhbtreeProfiler.getIdxNumber(true, false, 3, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, false, 3, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, false, 3, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, false, 3, 0)).append(",")
                .append(sifProfiler.getIdxNumber(true, false, 3, 0)).append("\n");
        sb.append("0.1,").append(dhbtreeProfiler.getIdxNumber(true, false, 4, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, false, 4, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, false, 4, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, false, 4, 0)).append(",")
                .append(sifProfiler.getIdxNumber(true, false, 4, 0)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie3_select_query_profiled_sidx_search_time.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateSelectQueryProfiledPidxSearchTime() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 select query profiled pidx search time report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(dhbtreeProfiler.getIdxNumber(true, false, 0, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, false, 0, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, false, 0, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, false, 0, 1)).append(",")
                .append(sifProfiler.getIdxNumber(true, false, 0, 1)).append("\n");
        sb.append("0.0001,").append(dhbtreeProfiler.getIdxNumber(true, false, 1, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, false, 1, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, false, 1, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, false, 1, 1)).append(",")
                .append(sifProfiler.getIdxNumber(true, false, 1, 1)).append("\n");
        sb.append("0.001,").append(dhbtreeProfiler.getIdxNumber(true, false, 2, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, false, 2, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, false, 2, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, false, 2, 1)).append(",")
                .append(sifProfiler.getIdxNumber(true, false, 2, 1)).append("\n");
        sb.append("0.01,").append(dhbtreeProfiler.getIdxNumber(true, false, 3, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, false, 3, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, false, 3, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, false, 3, 1)).append(",")
                .append(sifProfiler.getIdxNumber(true, false, 3, 1)).append("\n");
        sb.append("0.1,").append(dhbtreeProfiler.getIdxNumber(true, false, 4, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, false, 4, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, false, 4, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, false, 4, 1)).append(",")
                .append(sifProfiler.getIdxNumber(true, false, 4, 1)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie3_select_query_profiled_pidx_search_time.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateJoinQueryProfiledSidxSearchTime() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 join query profiled sidx search time report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(dhbtreeProfiler.getIdxNumber(true, true, 0, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, true, 0, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, true, 0, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, true, 0, 1)).append(",")
                .append(sifProfiler.getIdxNumber(true, true, 0, 1)).append("\n");
        sb.append("0.0001,").append(dhbtreeProfiler.getIdxNumber(true, true, 1, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, true, 1, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, true, 1, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, true, 1, 1)).append(",")
                .append(sifProfiler.getIdxNumber(true, true, 1, 1)).append("\n");
        sb.append("0.001,").append(dhbtreeProfiler.getIdxNumber(true, true, 2, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, true, 2, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, true, 2, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, true, 2, 1)).append(",")
                .append(sifProfiler.getIdxNumber(true, true, 2, 1)).append("\n");
        sb.append("0.01,").append(dhbtreeProfiler.getIdxNumber(true, true, 3, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, true, 3, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, true, 3, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, true, 3, 1)).append(",")
                .append(sifProfiler.getIdxNumber(true, true, 3, 1)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie3_join_query_profiled_sidx_search_time.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateJoinQueryProfiledPidxSearchTime() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 join query profiled pidx search time report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(dhbtreeProfiler.getIdxNumber(true, true, 0, 2)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, true, 0, 2)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, true, 0, 2)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, true, 0, 2)).append(",")
                .append(sifProfiler.getIdxNumber(true, true, 0, 2)).append("\n");
        sb.append("0.0001,").append(dhbtreeProfiler.getIdxNumber(true, true, 1, 2)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, true, 1, 2)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, true, 1, 2)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, true, 1, 2)).append(",")
                .append(sifProfiler.getIdxNumber(true, true, 1, 2)).append("\n");
        sb.append("0.001,").append(dhbtreeProfiler.getIdxNumber(true, true, 2, 2)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, true, 2, 2)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, true, 2, 2)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, true, 2, 2)).append(",")
                .append(sifProfiler.getIdxNumber(true, true, 2, 2)).append("\n");
        sb.append("0.01,").append(dhbtreeProfiler.getIdxNumber(true, true, 3, 2)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, true, 3, 2)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, true, 3, 2)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, true, 3, 2)).append(",")
                .append(sifProfiler.getIdxNumber(true, true, 3, 2)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie3_join_query_profiled_pidx_search_time.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateJoinQueryProfiledSeedPidxSearchTime() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 join query profiled query seed pidx search time report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(dhbtreeProfiler.getIdxNumber(true, true, 0, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, true, 0, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, true, 0, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, true, 0, 0)).append(",")
                .append(sifProfiler.getIdxNumber(true, true, 0, 0)).append("\n");
        sb.append("0.0001,").append(dhbtreeProfiler.getIdxNumber(true, true, 1, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, true, 1, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, true, 1, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, true, 1, 0)).append(",")
                .append(sifProfiler.getIdxNumber(true, true, 1, 0)).append("\n");
        sb.append("0.001,").append(dhbtreeProfiler.getIdxNumber(true, true, 2, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, true, 2, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, true, 2, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, true, 2, 0)).append(",")
                .append(sifProfiler.getIdxNumber(true, true, 2, 0)).append("\n");
        sb.append("0.01,").append(dhbtreeProfiler.getIdxNumber(true, true, 3, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(true, true, 3, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(true, true, 3, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(true, true, 3, 0)).append(",")
                .append(sifProfiler.getIdxNumber(true, true, 3, 0)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie3_join_query_profiled_seed_pidx_search_time.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateSelectQueryProfiledSidxCacheMiss() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 select query profiled sidx cache miss report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(dhbtreeProfiler.getIdxNumber(false, false, 0, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, false, 0, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, false, 0, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, false, 0, 0)).append(",")
                .append(sifProfiler.getIdxNumber(false, false, 0, 0)).append("\n");
        sb.append("0.0001,").append(dhbtreeProfiler.getIdxNumber(false, false, 1, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, false, 1, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, false, 1, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, false, 1, 0)).append(",")
                .append(sifProfiler.getIdxNumber(false, false, 1, 0)).append("\n");
        sb.append("0.001,").append(dhbtreeProfiler.getIdxNumber(false, false, 2, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, false, 2, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, false, 2, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, false, 2, 0)).append(",")
                .append(sifProfiler.getIdxNumber(false, false, 2, 0)).append("\n");
        sb.append("0.01,").append(dhbtreeProfiler.getIdxNumber(false, false, 3, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, false, 3, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, false, 3, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, false, 3, 0)).append(",")
                .append(sifProfiler.getIdxNumber(false, false, 3, 0)).append("\n");
        sb.append("0.1,").append(dhbtreeProfiler.getIdxNumber(false, false, 4, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, false, 4, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, false, 4, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, false, 4, 0)).append(",")
                .append(sifProfiler.getIdxNumber(false, false, 4, 0)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie3_select_query_profiled_sidx_cache_miss.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateSelectQueryProfiledPidxCacheMiss() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 select query profiled pidx cache miss report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(dhbtreeProfiler.getIdxNumber(false, false, 0, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, false, 0, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, false, 0, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, false, 0, 1)).append(",")
                .append(sifProfiler.getIdxNumber(false, false, 0, 1)).append("\n");
        sb.append("0.0001,").append(dhbtreeProfiler.getIdxNumber(false, false, 1, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, false, 1, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, false, 1, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, false, 1, 1)).append(",")
                .append(sifProfiler.getIdxNumber(false, false, 1, 1)).append("\n");
        sb.append("0.001,").append(dhbtreeProfiler.getIdxNumber(false, false, 2, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, false, 2, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, false, 2, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, false, 2, 1)).append(",")
                .append(sifProfiler.getIdxNumber(false, false, 2, 1)).append("\n");
        sb.append("0.01,").append(dhbtreeProfiler.getIdxNumber(false, false, 3, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, false, 3, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, false, 3, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, false, 3, 1)).append(",")
                .append(sifProfiler.getIdxNumber(false, false, 3, 1)).append("\n");
        sb.append("0.1,").append(dhbtreeProfiler.getIdxNumber(false, false, 4, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, false, 4, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, false, 4, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, false, 4, 1)).append(",")
                .append(sifProfiler.getIdxNumber(false, false, 4, 1)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie3_select_query_profiled_pidx_cache_miss.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateJoinQueryProfiledSidxCacheMiss() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 join query profiled sidx cache miss report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(dhbtreeProfiler.getIdxNumber(false, true, 0, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, true, 0, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, true, 0, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, true, 0, 1)).append(",")
                .append(sifProfiler.getIdxNumber(false, true, 0, 1)).append("\n");
        sb.append("0.0001,").append(dhbtreeProfiler.getIdxNumber(false, true, 1, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, true, 1, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, true, 1, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, true, 1, 1)).append(",")
                .append(sifProfiler.getIdxNumber(false, true, 1, 1)).append("\n");
        sb.append("0.001,").append(dhbtreeProfiler.getIdxNumber(false, true, 2, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, true, 2, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, true, 2, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, true, 2, 1)).append(",")
                .append(sifProfiler.getIdxNumber(false, true, 2, 1)).append("\n");
        sb.append("0.01,").append(dhbtreeProfiler.getIdxNumber(false, true, 3, 1)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, true, 3, 1)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, true, 3, 1)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, true, 3, 1)).append(",")
                .append(sifProfiler.getIdxNumber(false, true, 3, 1)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie3_join_query_profiled_sidx_cache_miss.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateJoinQueryProfiledPidxCacheMiss() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 join query profiled pidx cache miss report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(dhbtreeProfiler.getIdxNumber(false, true, 0, 2)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, true, 0, 2)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, true, 0, 2)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, true, 0, 2)).append(",")
                .append(sifProfiler.getIdxNumber(false, true, 0, 2)).append("\n");
        sb.append("0.0001,").append(dhbtreeProfiler.getIdxNumber(false, true, 1, 2)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, true, 1, 2)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, true, 1, 2)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, true, 1, 2)).append(",")
                .append(sifProfiler.getIdxNumber(false, true, 1, 2)).append("\n");
        sb.append("0.001,").append(dhbtreeProfiler.getIdxNumber(false, true, 2, 2)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, true, 2, 2)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, true, 2, 2)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, true, 2, 2)).append(",")
                .append(sifProfiler.getIdxNumber(false, true, 2, 2)).append("\n");
        sb.append("0.01,").append(dhbtreeProfiler.getIdxNumber(false, true, 3, 2)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, true, 3, 2)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, true, 3, 2)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, true, 3, 2)).append(",")
                .append(sifProfiler.getIdxNumber(false, true, 3, 2)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie3_join_query_profiled_pidx_cache_miss.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateJoinQueryProfiledSeedPidxCacheMiss() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 join query profiled query seed pidx search time report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(dhbtreeProfiler.getIdxNumber(false, true, 0, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, true, 0, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, true, 0, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, true, 0, 0)).append(",")
                .append(sifProfiler.getIdxNumber(false, true, 0, 0)).append("\n");
        sb.append("0.0001,").append(dhbtreeProfiler.getIdxNumber(false, true, 1, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, true, 1, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, true, 1, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, true, 1, 0)).append(",")
                .append(sifProfiler.getIdxNumber(false, true, 1, 0)).append("\n");
        sb.append("0.001,").append(dhbtreeProfiler.getIdxNumber(false, true, 2, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, true, 2, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, true, 2, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, true, 2, 0)).append(",")
                .append(sifProfiler.getIdxNumber(false, true, 2, 0)).append("\n");
        sb.append("0.01,").append(dhbtreeProfiler.getIdxNumber(false, true, 3, 0)).append(",")
                .append(dhvbtreeProfiler.getIdxNumber(false, true, 3, 0)).append(",")
                .append(rtreeProfiler.getIdxNumber(false, true, 3, 0)).append(",")
                .append(shbtreeProfiler.getIdxNumber(false, true, 3, 0)).append(",")
                .append(sifProfiler.getIdxNumber(false, true, 3, 0)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie3_join_query_profiled_seed_pidx_cache_miss.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateSelectQueryProfiledFalsePositive() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 select query profiled false positive raw report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(dhbtreeProfiler.getFalsePositives(false, 0)).append(",")
                .append(dhvbtreeProfiler.getFalsePositives(false, 0)).append(",")
                .append(rtreeProfiler.getFalsePositives(false, 0)).append(",")
                .append(shbtreeProfiler.getFalsePositives(false, 0)).append(",")
                .append(sifProfiler.getFalsePositives(false, 0)).append("\n");
        sb.append("0.0001,").append(dhbtreeProfiler.getFalsePositives(false, 1)).append(",")
                .append(dhvbtreeProfiler.getFalsePositives(false, 1)).append(",")
                .append(rtreeProfiler.getFalsePositives(false, 1)).append(",")
                .append(shbtreeProfiler.getFalsePositives(false, 1)).append(",")
                .append(sifProfiler.getFalsePositives(false, 1)).append("\n");
        sb.append("0.001,").append(dhbtreeProfiler.getFalsePositives(false, 2)).append(",")
                .append(dhvbtreeProfiler.getFalsePositives(false, 2)).append(",")
                .append(rtreeProfiler.getFalsePositives(false, 2)).append(",")
                .append(shbtreeProfiler.getFalsePositives(false, 2)).append(",")
                .append(sifProfiler.getFalsePositives(false, 2)).append("\n");
        sb.append("0.01,").append(dhbtreeProfiler.getFalsePositives(false, 3)).append(",")
                .append(dhvbtreeProfiler.getFalsePositives(false, 3)).append(",")
                .append(rtreeProfiler.getFalsePositives(false, 3)).append(",")
                .append(shbtreeProfiler.getFalsePositives(false, 3)).append(",")
                .append(sifProfiler.getFalsePositives(false, 3)).append("\n");
        sb.append("0.1,").append(dhbtreeProfiler.getFalsePositives(false, 4)).append(",")
                .append(dhvbtreeProfiler.getFalsePositives(false, 4)).append(",")
                .append(rtreeProfiler.getFalsePositives(false, 4)).append(",")
                .append(shbtreeProfiler.getFalsePositives(false, 4)).append(",")
                .append(sifProfiler.getFalsePositives(false, 4)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie3_select_query_profiled_false_positive_raw.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
        generateFalsePositive(outputFilePath + "sie3_select_query_profiled_false_positive_raw.txt", outputFilePath
                + "sie3_select_query_result_count.txt", outputFilePath
                + "sie3_select_query_profiled_false_positive.txt", false);
    }

    private void generateFalsePositive(String falsePositveFile, String queryResultCountFile, String outputFile,
            boolean isJoin) throws IOException {

        String[] fps, rcs;
        sb.setLength(0);

        BufferedReader brFalsePositive = new BufferedReader(new FileReader(falsePositveFile));
        BufferedReader brQueryResultCount = new BufferedReader(new FileReader(queryResultCountFile));

        //discard two head lines
        brFalsePositive.readLine();
        brFalsePositive.readLine();
        brQueryResultCount.readLine();
        brQueryResultCount.readLine();

        int radiusCount = isJoin ? 4 : 5;
        int partitionCount = 24;
        String[] radius = { "0.00001", "0.0001", "0.001", "0.01", "0.1" };

        if (isJoin) {
            sb.append("# sie3 join query profiled false positive report\n");
        } else {
            sb.append("# sie3 select query profiled false positive report\n");
        }
        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");

        for (int i = 0; i < radiusCount; i++) {
            fps = brFalsePositive.readLine().split(",");
            rcs = brQueryResultCount.readLine().split(",");
            //false positive count
            sb.append(radius[i])
                    .append(",")
                    .append(((Double.parseDouble(fps[1]) * partitionCount) - Double.parseDouble(rcs[1]))
                            / partitionCount)
                    .append(",")
                    .append(((Double.parseDouble(fps[2]) * partitionCount) - Double.parseDouble(rcs[2]))
                            / partitionCount)
                    .append(",")
                    .append(((Double.parseDouble(fps[3]) * partitionCount) - Double.parseDouble(rcs[3]))
                            / partitionCount)
                    .append(",")
                    .append(((Double.parseDouble(fps[4]) * partitionCount) - Double.parseDouble(rcs[4]))
                            / partitionCount)
                    .append(",")
                    .append(((Double.parseDouble(fps[5]) * partitionCount) - Double.parseDouble(rcs[5]))
                            / partitionCount).append("\n");
            //false positive rate
            //            sb.append(radius[i])
            //            .append(",").append(((Double.parseDouble(fps[1]) * partitionCount) - Double.parseDouble(rcs[1]))/(Double.parseDouble(fps[1]) * partitionCount))
            //            .append(",").append(((Double.parseDouble(fps[2]) * partitionCount) - Double.parseDouble(rcs[2]))/(Double.parseDouble(fps[2]) * partitionCount))
            //            .append(",").append(((Double.parseDouble(fps[3]) * partitionCount) - Double.parseDouble(rcs[3]))/(Double.parseDouble(fps[3]) * partitionCount))
            //            .append(",").append(((Double.parseDouble(fps[4]) * partitionCount) - Double.parseDouble(rcs[4]))/(Double.parseDouble(fps[4]) * partitionCount))
            //            .append(",").append(((Double.parseDouble(fps[5]) * partitionCount) - Double.parseDouble(rcs[5]))/(Double.parseDouble(fps[5]) * partitionCount))
            //            .append("\n");
        }
        brFalsePositive.close();
        brQueryResultCount.close();

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFile);
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateJoinQueryProfiledFalsePositive() throws Exception {
        sb.setLength(0);
        sb.append("# sie3 join query profiled false positive raw report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(dhbtreeProfiler.getFalsePositives(true, 0)).append(",")
                .append(dhvbtreeProfiler.getFalsePositives(true, 0)).append(",")
                .append(rtreeProfiler.getFalsePositives(true, 0)).append(",")
                .append(shbtreeProfiler.getFalsePositives(true, 0)).append(",")
                .append(sifProfiler.getFalsePositives(true, 0)).append("\n");
        sb.append("0.0001,").append(dhbtreeProfiler.getFalsePositives(true, 1)).append(",")
                .append(dhvbtreeProfiler.getFalsePositives(true, 1)).append(",")
                .append(rtreeProfiler.getFalsePositives(true, 1)).append(",")
                .append(shbtreeProfiler.getFalsePositives(true, 1)).append(",")
                .append(sifProfiler.getFalsePositives(true, 1)).append("\n");
        sb.append("0.001,").append(dhbtreeProfiler.getFalsePositives(true, 2)).append(",")
                .append(dhvbtreeProfiler.getFalsePositives(true, 2)).append(",")
                .append(rtreeProfiler.getFalsePositives(true, 2)).append(",")
                .append(shbtreeProfiler.getFalsePositives(true, 2)).append(",")
                .append(sifProfiler.getFalsePositives(true, 2)).append("\n");
        sb.append("0.01,").append(dhbtreeProfiler.getFalsePositives(true, 3)).append(",")
                .append(dhvbtreeProfiler.getFalsePositives(true, 3)).append(",")
                .append(rtreeProfiler.getFalsePositives(true, 3)).append(",")
                .append(shbtreeProfiler.getFalsePositives(true, 3)).append(",")
                .append(sifProfiler.getFalsePositives(true, 3)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie3_join_query_profiled_false_positive_raw.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        generateFalsePositive(outputFilePath + "sie3_join_query_profiled_false_positive_raw.txt", outputFilePath
                + "sie3_join_query_result_count.txt", outputFilePath + "sie3_join_query_profiled_false_positive.txt",
                true);
    }

}
