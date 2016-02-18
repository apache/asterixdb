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

import java.io.FileOutputStream;
import java.util.ArrayList;

public class SIE2ReportBuilderRunner {
    String expHomePath = "/Users/kisskys/workspace/asterix_master/resultLog/MemBuf3g-DiskBuf3g-Lsev-Jvm7g-Lock0g/exp2-4/";
    String runLogFileName = "run-exp2-4.log";
    String outputFilePath = "/Users/kisskys/workspace/asterix_master/resultLog/MemBuf3g-DiskBuf3g-Lsev-Jvm7g-Lock0g/result-report/";

    SIE2ReportBuilder sie2Dhbtree = new SIE2ReportBuilder(expHomePath, "SpatialIndexExperiment2Dhbtree", runLogFileName);
    SIE2ReportBuilder sie2Dhvbtree = new SIE2ReportBuilder(expHomePath, "SpatialIndexExperiment2Dhvbtree",
            runLogFileName);
    SIE2ReportBuilder sie2Rtree = new SIE2ReportBuilder(expHomePath, "SpatialIndexExperiment2Rtree", runLogFileName);
    SIE2ReportBuilder sie2Shbtree = new SIE2ReportBuilder(expHomePath, "SpatialIndexExperiment2Shbtree", runLogFileName);
    SIE2ReportBuilder sie2Sif = new SIE2ReportBuilder(expHomePath, "SpatialIndexExperiment2Sif", runLogFileName);

    StringBuilder sb = new StringBuilder();

    /**
     * generate sie2_overall_insert_ps.txt
     */
    public void generateOverallInsertPS() throws Exception {
        int targetRound = 721; //(3600 seconds / 5seconds) + 1

        ArrayList<Long> ipsListDhbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListDhvbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListRtree = new ArrayList<Long>();
        ArrayList<Long> ipsListShbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListSif = new ArrayList<Long>();
        sie2Dhbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListDhbtree);
        sie2Dhvbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListDhvbtree);
        sie2Rtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListRtree);
        sie2Shbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListShbtree);
        sie2Sif.getAllNodesAccumulatedInsertPS(targetRound, ipsListSif);

        sb.setLength(0);
        sb.append("# sie2 60min inserts per second report\n");
        sb.append("index type, InsertPS\n");
        sb.append("dhbtree,").append(ipsListDhbtree.get(targetRound - 1)).append("\n");
        sb.append("dhvbtree,").append(ipsListDhvbtree.get(targetRound - 1)).append("\n");
        sb.append("rtree,").append(ipsListRtree.get(targetRound - 1)).append("\n");
        sb.append("shbtree,").append(ipsListShbtree.get(targetRound - 1)).append("\n");
        sb.append("sif,").append(ipsListSif.get(targetRound - 1)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie2_overall_insert_ps.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        ipsListDhbtree.clear();
        ipsListDhvbtree.clear();
        ipsListRtree.clear();
        ipsListShbtree.clear();
        ipsListSif.clear();
    }

    /**
     * generate sie2_accumulated_insert_ps.txt
     */
    public void generateAccumulatedInsertPS() throws Exception {
        int targetRound = 721; //(3600 seconds / 5seconds) + 1
        int roundInterval = 5;

        ArrayList<Long> ipsListDhbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListDhvbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListRtree = new ArrayList<Long>();
        ArrayList<Long> ipsListShbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListSif = new ArrayList<Long>();
        sie2Dhbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListDhbtree);
        sie2Dhvbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListDhvbtree);
        sie2Rtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListRtree);
        sie2Shbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListShbtree);
        sie2Sif.getAllNodesAccumulatedInsertPS(targetRound, ipsListSif);

        sb.setLength(0);
        sb.append("# sie2 accumulated inserts per second report\n");
        sb.append("# time, dhbtree, dhvbtree, rtree, shbtree, sif\n");

        for (int i = 0; i < targetRound; i++) {
            sb.append("" + (i * roundInterval) + "," + ipsListDhbtree.get(i) + "," + ipsListDhvbtree.get(i) + ","
                    + ipsListRtree.get(i) + "," + ipsListShbtree.get(i) + "," + ipsListSif.get(i) + "\n");
        }
        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie2_accumulated_insert_ps.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        ipsListDhbtree.clear();
        ipsListDhvbtree.clear();
        ipsListRtree.clear();
        ipsListShbtree.clear();
        ipsListSif.clear();
    }

    public void generateQueryPS() throws Exception {
        int minutes = 60;
        sb.setLength(0);
        sb.append("# sie2 queries per second report\n");
        sb.append("index type, QueryPS\n");
        sb.append("dhbtree,").append(sie2Dhbtree.getQueryPS(minutes)).append("\n");
        sb.append("dhvbtree,").append(sie2Dhvbtree.getQueryPS(minutes)).append("\n");
        sb.append("rtree,").append(sie2Rtree.getQueryPS(minutes)).append("\n");
        sb.append("shbtree,").append(sie2Shbtree.getQueryPS(minutes)).append("\n");
        sb.append("sif,").append(sie2Sif.getQueryPS(minutes)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie2_query_ps.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateAverageQueryResultCount() throws Exception {
        sb.setLength(0);
        sb.append("# sie2 average query result count report\n");
        sb.append("index type, query result count\n");
        sb.append("dhbtree,").append(sie2Dhbtree.getAverageQueryResultCount()).append("\n");
        sb.append("dhvbtree,").append(sie2Dhvbtree.getAverageQueryResultCount()).append("\n");
        sb.append("rtree,").append(sie2Rtree.getAverageQueryResultCount()).append("\n");
        sb.append("shbtree,").append(sie2Shbtree.getAverageQueryResultCount()).append("\n");
        sb.append("sif,").append(sie2Sif.getAverageQueryResultCount()).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie2_average_query_result_count.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateAverageQueryResponseTime() throws Exception {
        sb.setLength(0);
        sb.append("# sie2 average query response time report\n");
        sb.append("index type, query response time\n");
        sb.append("dhbtree,").append(sie2Dhbtree.getAverageQueryResponseTime()).append("\n");
        sb.append("dhvbtree,").append(sie2Dhvbtree.getAverageQueryResponseTime()).append("\n");
        sb.append("rtree,").append(sie2Rtree.getAverageQueryResponseTime()).append("\n");
        sb.append("shbtree,").append(sie2Shbtree.getAverageQueryResponseTime()).append("\n");
        sb.append("sif,").append(sie2Sif.getAverageQueryResponseTime()).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie2_average_query_response_time.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateInstantaneousInsertPS() throws Exception {
        for (int i = 0; i < 8; i++) {
            sb.setLength(0);
            sb.append("# sie2 instantaneous inserts per second report\n");
            sb.append(sie2Dhbtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie2_instantaneous_insert_ps_dhbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 8; i++) {
            sb.setLength(0);
            sb.append("# sie2 instantaneous inserts per second report\n");
            sb.append(sie2Dhvbtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie2_instantaneous_insert_ps_dhvbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 8; i++) {
            sb.setLength(0);
            sb.append("# sie2 instantaneous inserts per second report\n");
            sb.append(sie2Rtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie2_instantaneous_insert_ps_rtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 8; i++) {
            sb.setLength(0);
            sb.append("# sie2 instantaneous inserts per second report\n");
            sb.append(sie2Shbtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie2_instantaneous_insert_ps_shbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 8; i++) {
            sb.setLength(0);
            sb.append("# sie2 instantaneous inserts per second report\n");
            sb.append(sie2Sif.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie2_instantaneous_insert_ps_sif_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
    }

    public void generateGanttInstantaneousInsertPS() throws Exception {
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie2 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie2Dhbtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie2_gantt_1node_instantaneous_insert_ps_dhbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie2 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie2Dhvbtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie2_gantt_1node_instantaneous_insert_ps_dhvbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie2 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie2Rtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie2_gantt_1node_instantaneous_insert_ps_rtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie2 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie2Shbtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie2_gantt_1node_instantaneous_insert_ps_shbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie2 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie2Sif.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie2_gantt_1node_instantaneous_insert_ps_sif_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }

        long dataGenStartTime = sie2Dhbtree.getDataGenStartTimeStamp();
        NCLogReportBuilder ncLogReportBuilder = new NCLogReportBuilder(expHomePath
                + "SpatialIndexExperiment2Dhbtree/logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie2_gantt_1node_flush_merge_dhbtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        dataGenStartTime = sie2Dhvbtree.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(expHomePath + "SpatialIndexExperiment2Dhvbtree/logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie2_gantt_1node_flush_merge_dhvbtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        dataGenStartTime = sie2Rtree.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(expHomePath + "SpatialIndexExperiment2Rtree/logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie2_gantt_1node_flush_merge_rtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        dataGenStartTime = sie2Shbtree.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(expHomePath + "SpatialIndexExperiment2Shbtree/logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie2_gantt_1node_flush_merge_shbtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        dataGenStartTime = sie2Sif.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(expHomePath + "SpatialIndexExperiment2Sif/logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie2_gantt_1node_flush_merge_sif.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateSelectQueryResponseTime() throws Exception {
        sb.setLength(0);
        sb.append("# sie2 select query response time report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(sie2Dhbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie2Rtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie2Shbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie2Sif.getSelectQueryResponseTime(0)).append("\n");
        sb.append("0.0001,").append(sie2Dhbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie2Rtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie2Shbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie2Sif.getSelectQueryResponseTime(1)).append("\n");
        sb.append("0.001,").append(sie2Dhbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie2Rtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie2Shbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie2Sif.getSelectQueryResponseTime(2)).append("\n");
        sb.append("0.01,").append(sie2Dhbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie2Rtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie2Shbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie2Sif.getSelectQueryResponseTime(3)).append("\n");
        sb.append("0.1,").append(sie2Dhbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie2Rtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie2Shbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie2Sif.getSelectQueryResponseTime(4)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie2_select_query_response_time.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        sb.setLength(0);
        sb.append("# sie2 select query response time report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(sie2Dhbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie2Rtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie2Shbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie2Sif.getSelectQueryResponseTime(0)).append("\n");
        sb.append("0.0001,").append(sie2Dhbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie2Rtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie2Shbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie2Sif.getSelectQueryResponseTime(1)).append("\n");
        sb.append("0.001,").append(sie2Dhbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie2Rtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie2Shbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie2Sif.getSelectQueryResponseTime(2)).append("\n");

        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie2_select_query_response_time1.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        sb.setLength(0);
        sb.append("# sie2 select query response time 2 report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.01,").append(sie2Dhbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie2Rtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie2Shbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie2Sif.getSelectQueryResponseTime(3)).append("\n");
        sb.append("0.1,").append(sie2Dhbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie2Rtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie2Shbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie2Sif.getSelectQueryResponseTime(4)).append("\n");

        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie2_select_query_response_time2.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateSelectQueryResultCount() throws Exception {

        sb.setLength(0);
        sb.append("# sie2 select query result count report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(sie2Dhbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie2Rtree.getSelectQueryResultCount(0)).append(",")
                .append(sie2Shbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie2Sif.getSelectQueryResultCount(0)).append("\n");
        sb.append("0.0001,").append(sie2Dhbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie2Rtree.getSelectQueryResultCount(1)).append(",")
                .append(sie2Shbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie2Sif.getSelectQueryResultCount(1)).append("\n");
        sb.append("0.001,").append(sie2Dhbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie2Rtree.getSelectQueryResultCount(2)).append(",")
                .append(sie2Shbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie2Sif.getSelectQueryResultCount(2)).append("\n");
        sb.append("0.01,").append(sie2Dhbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie2Rtree.getSelectQueryResultCount(3)).append(",")
                .append(sie2Shbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie2Sif.getSelectQueryResultCount(3)).append("\n");
        sb.append("0.1,").append(sie2Dhbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie2Rtree.getSelectQueryResultCount(4)).append(",")
                .append(sie2Shbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie2Sif.getSelectQueryResultCount(4)).append("\n");

        FileOutputStream fos = ReportBuilderHelper
                .openOutputFile(outputFilePath + "sie2_select_query_result_count.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        sb.setLength(0);
        sb.append("# sie2 select query result count 1 report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(sie2Dhbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie2Rtree.getSelectQueryResultCount(0)).append(",")
                .append(sie2Shbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie2Sif.getSelectQueryResultCount(0)).append("\n");
        sb.append("0.0001,").append(sie2Dhbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie2Rtree.getSelectQueryResultCount(1)).append(",")
                .append(sie2Shbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie2Sif.getSelectQueryResultCount(1)).append("\n");
        sb.append("0.001,").append(sie2Dhbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie2Rtree.getSelectQueryResultCount(2)).append(",")
                .append(sie2Shbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie2Sif.getSelectQueryResultCount(2)).append("\n");

        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie2_select_query_result_count1.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        sb.setLength(0);
        sb.append("# sie2 select query result count 2 report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.01,").append(sie2Dhbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie2Rtree.getSelectQueryResultCount(3)).append(",")
                .append(sie2Shbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie2Sif.getSelectQueryResultCount(3)).append("\n");
        sb.append("0.1,").append(sie2Dhbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie2Dhvbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie2Rtree.getSelectQueryResultCount(4)).append(",")
                .append(sie2Shbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie2Sif.getSelectQueryResultCount(4)).append("\n");

        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie2_select_query_result_count2.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }
}
