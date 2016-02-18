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

public class SIE5ReportBuilderRunner {
    String expHomePath = "/Users/kisskys/workspace/asterix_master/resultLog/MemBuf3g-DiskBuf3g-Lsev-Jvm7g-Lock0g/exp5-4/";
    String runLogFileName = "run-exp5-4.log";
    String outputFilePath = "/Users/kisskys/workspace/asterix_master/resultLog/MemBuf3g-DiskBuf3g-Lsev-Jvm7g-Lock0g/result-report/";

    SIE2ReportBuilder sie5Dhbtree = new SIE2ReportBuilder(expHomePath, "SpatialIndexExperiment5Dhbtree", runLogFileName);
    SIE2ReportBuilder sie5Dhvbtree = new SIE2ReportBuilder(expHomePath, "SpatialIndexExperiment5Dhvbtree",
            runLogFileName);
    SIE2ReportBuilder sie5Rtree = new SIE2ReportBuilder(expHomePath, "SpatialIndexExperiment5Rtree", runLogFileName);
    SIE2ReportBuilder sie5Shbtree = new SIE2ReportBuilder(expHomePath, "SpatialIndexExperiment5Shbtree", runLogFileName);
    SIE2ReportBuilder sie5Sif = new SIE2ReportBuilder(expHomePath, "SpatialIndexExperiment5Sif", runLogFileName);

    StringBuilder sb = new StringBuilder();

    /**
     * generate sie5_overall_insert_ps.txt
     */
    public void generateOverallInsertPS() throws Exception {
        int targetRound = 721; //(3600 seconds / 5seconds) + 1

        ArrayList<Long> ipsListDhbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListDhvbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListRtree = new ArrayList<Long>();
        ArrayList<Long> ipsListShbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListSif = new ArrayList<Long>();
        sie5Dhbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListDhbtree);
        sie5Dhvbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListDhvbtree);
        sie5Rtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListRtree);
        sie5Shbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListShbtree);
        sie5Sif.getAllNodesAccumulatedInsertPS(targetRound, ipsListSif);

        sb.setLength(0);
        sb.append("# sie5 60min inserts per second report\n");
        sb.append("index type, InsertPS\n");
        sb.append("dhbtree,").append(ipsListDhbtree.get(targetRound - 1)).append("\n");
        sb.append("dhvbtree,").append(ipsListDhvbtree.get(targetRound - 1)).append("\n");
        sb.append("rtree,").append(ipsListRtree.get(targetRound - 1)).append("\n");
        sb.append("shbtree,").append(ipsListShbtree.get(targetRound - 1)).append("\n");
        sb.append("sif,").append(ipsListSif.get(targetRound - 1)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie5_overall_insert_ps.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        ipsListDhbtree.clear();
        ipsListDhvbtree.clear();
        ipsListRtree.clear();
        ipsListShbtree.clear();
        ipsListSif.clear();
    }

    /**
     * generate sie5_accumulated_insert_ps.txt
     */
    public void generateAccumulatedInsertPS() throws Exception {
        int targetRound = 721; //(3600 seconds / 5seconds) + 1
        int roundInterval = 5;

        ArrayList<Long> ipsListDhbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListDhvbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListRtree = new ArrayList<Long>();
        ArrayList<Long> ipsListShbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListSif = new ArrayList<Long>();
        sie5Dhbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListDhbtree);
        sie5Dhvbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListDhvbtree);
        sie5Rtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListRtree);
        sie5Shbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListShbtree);
        sie5Sif.getAllNodesAccumulatedInsertPS(targetRound, ipsListSif);

        sb.setLength(0);
        sb.append("# sie5 accumulated inserts per second report\n");
        sb.append("# time, dhbtree, dhvbtree, rtree, shbtree, sif\n");

        for (int i = 0; i < targetRound; i++) {
            sb.append("" + (i * roundInterval) + "," + ipsListDhbtree.get(i) + "," + ipsListDhvbtree.get(i) + ","
                    + ipsListRtree.get(i) + "," + ipsListShbtree.get(i) + "," + ipsListSif.get(i) + "\n");
        }
        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie5_accumulated_insert_ps.txt");
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
        sb.append("# sie5 queries per second report\n");
        sb.append("index type, QueryPS\n");
        sb.append("dhbtree,").append(sie5Dhbtree.getQueryPS(minutes)).append("\n");
        sb.append("dhvbtree,").append(sie5Dhvbtree.getQueryPS(minutes)).append("\n");
        sb.append("rtree,").append(sie5Rtree.getQueryPS(minutes)).append("\n");
        sb.append("shbtree,").append(sie5Shbtree.getQueryPS(minutes)).append("\n");
        sb.append("sif,").append(sie5Sif.getQueryPS(minutes)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie5_query_ps.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateAverageQueryResultCount() throws Exception {
        sb.setLength(0);
        sb.append("# sie5 average query result count report\n");
        sb.append("index type, query result count\n");
        sb.append("dhbtree,").append(sie5Dhbtree.getAverageQueryResultCount()).append("\n");
        sb.append("dhvbtree,").append(sie5Dhvbtree.getAverageQueryResultCount()).append("\n");
        sb.append("rtree,").append(sie5Rtree.getAverageQueryResultCount()).append("\n");
        sb.append("shbtree,").append(sie5Shbtree.getAverageQueryResultCount()).append("\n");
        sb.append("sif,").append(sie5Sif.getAverageQueryResultCount()).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie5_average_query_result_count.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateAverageQueryResponseTime() throws Exception {
        sb.setLength(0);
        sb.append("# sie5 average query response time report\n");
        sb.append("index type, query response time\n");
        sb.append("dhbtree,").append(sie5Dhbtree.getAverageQueryResponseTime()).append("\n");
        sb.append("dhvbtree,").append(sie5Dhvbtree.getAverageQueryResponseTime()).append("\n");
        sb.append("rtree,").append(sie5Rtree.getAverageQueryResponseTime()).append("\n");
        sb.append("shbtree,").append(sie5Shbtree.getAverageQueryResponseTime()).append("\n");
        sb.append("sif,").append(sie5Sif.getAverageQueryResponseTime()).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie5_average_query_response_time.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateInstantaneousInsertPS() throws Exception {
        for (int i = 0; i < 8; i++) {
            sb.setLength(0);
            sb.append("# sie5 instantaneous inserts per second report\n");
            sb.append(sie5Dhbtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie5_instantaneous_insert_ps_dhbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 8; i++) {
            sb.setLength(0);
            sb.append("# sie5 instantaneous inserts per second report\n");
            sb.append(sie5Dhvbtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie5_instantaneous_insert_ps_dhvbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 8; i++) {
            sb.setLength(0);
            sb.append("# sie5 instantaneous inserts per second report\n");
            sb.append(sie5Rtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie5_instantaneous_insert_ps_rtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 8; i++) {
            sb.setLength(0);
            sb.append("# sie5 instantaneous inserts per second report\n");
            sb.append(sie5Shbtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie5_instantaneous_insert_ps_shbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 8; i++) {
            sb.setLength(0);
            sb.append("# sie5 instantaneous inserts per second report\n");
            sb.append(sie5Sif.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie5_instantaneous_insert_ps_sif_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
    }

    public void generateGanttInstantaneousInsertPS() throws Exception {
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie5 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie5Dhbtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie5_gantt_1node_instantaneous_insert_ps_dhbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie5 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie5Dhvbtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie5_gantt_1node_instantaneous_insert_ps_dhvbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie5 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie5Rtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie5_gantt_1node_instantaneous_insert_ps_rtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie5 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie5Shbtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie5_gantt_1node_instantaneous_insert_ps_shbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie5 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie5Sif.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie5_gantt_1node_instantaneous_insert_ps_sif_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }

        long dataGenStartTime = sie5Dhbtree.getDataGenStartTimeStamp();
        NCLogReportBuilder ncLogReportBuilder = new NCLogReportBuilder(expHomePath
                + "SpatialIndexExperiment5Dhbtree/logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie5_gantt_1node_flush_merge_dhbtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        dataGenStartTime = sie5Dhvbtree.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(expHomePath + "SpatialIndexExperiment5Dhvbtree/logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie5_gantt_1node_flush_merge_dhvbtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        dataGenStartTime = sie5Rtree.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(expHomePath + "SpatialIndexExperiment5Rtree/logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie5_gantt_1node_flush_merge_rtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        dataGenStartTime = sie5Shbtree.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(expHomePath + "SpatialIndexExperiment5Shbtree/logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie5_gantt_1node_flush_merge_shbtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        dataGenStartTime = sie5Sif.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(expHomePath + "SpatialIndexExperiment5Sif/logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie5_gantt_1node_flush_merge_sif.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateSelectQueryResponseTime() throws Exception {
        sb.setLength(0);
        sb.append("# sie5 select query response time report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(sie5Dhbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie5Rtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie5Shbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie5Sif.getSelectQueryResponseTime(0)).append("\n");
        sb.append("0.0001,").append(sie5Dhbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie5Rtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie5Shbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie5Sif.getSelectQueryResponseTime(1)).append("\n");
        sb.append("0.001,").append(sie5Dhbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie5Rtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie5Shbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie5Sif.getSelectQueryResponseTime(2)).append("\n");
        sb.append("0.01,").append(sie5Dhbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie5Rtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie5Shbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie5Sif.getSelectQueryResponseTime(3)).append("\n");
        sb.append("0.1,").append(sie5Dhbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie5Rtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie5Shbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie5Sif.getSelectQueryResponseTime(4)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie5_select_query_response_time.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        sb.setLength(0);
        sb.append("# sie5 select query response time report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(sie5Dhbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie5Rtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie5Shbtree.getSelectQueryResponseTime(0)).append(",")
                .append(sie5Sif.getSelectQueryResponseTime(0)).append("\n");
        sb.append("0.0001,").append(sie5Dhbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie5Rtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie5Shbtree.getSelectQueryResponseTime(1)).append(",")
                .append(sie5Sif.getSelectQueryResponseTime(1)).append("\n");
        sb.append("0.001,").append(sie5Dhbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie5Rtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie5Shbtree.getSelectQueryResponseTime(2)).append(",")
                .append(sie5Sif.getSelectQueryResponseTime(2)).append("\n");

        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie5_select_query_response_time1.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        sb.setLength(0);
        sb.append("# sie5 select query response time 2 report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.01,").append(sie5Dhbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie5Rtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie5Shbtree.getSelectQueryResponseTime(3)).append(",")
                .append(sie5Sif.getSelectQueryResponseTime(3)).append("\n");
        sb.append("0.1,").append(sie5Dhbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie5Rtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie5Shbtree.getSelectQueryResponseTime(4)).append(",")
                .append(sie5Sif.getSelectQueryResponseTime(4)).append("\n");

        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie5_select_query_response_time2.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateSelectQueryResultCount() throws Exception {

        sb.setLength(0);
        sb.append("# sie5 select query result count report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(sie5Dhbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie5Rtree.getSelectQueryResultCount(0)).append(",")
                .append(sie5Shbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie5Sif.getSelectQueryResultCount(0)).append("\n");
        sb.append("0.0001,").append(sie5Dhbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie5Rtree.getSelectQueryResultCount(1)).append(",")
                .append(sie5Shbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie5Sif.getSelectQueryResultCount(1)).append("\n");
        sb.append("0.001,").append(sie5Dhbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie5Rtree.getSelectQueryResultCount(2)).append(",")
                .append(sie5Shbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie5Sif.getSelectQueryResultCount(2)).append("\n");
        sb.append("0.01,").append(sie5Dhbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie5Rtree.getSelectQueryResultCount(3)).append(",")
                .append(sie5Shbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie5Sif.getSelectQueryResultCount(3)).append("\n");
        sb.append("0.1,").append(sie5Dhbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie5Rtree.getSelectQueryResultCount(4)).append(",")
                .append(sie5Shbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie5Sif.getSelectQueryResultCount(4)).append("\n");

        FileOutputStream fos = ReportBuilderHelper
                .openOutputFile(outputFilePath + "sie5_select_query_result_count.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        sb.setLength(0);
        sb.append("# sie5 select query result count 1 report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.00001,").append(sie5Dhbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie5Rtree.getSelectQueryResultCount(0)).append(",")
                .append(sie5Shbtree.getSelectQueryResultCount(0)).append(",")
                .append(sie5Sif.getSelectQueryResultCount(0)).append("\n");
        sb.append("0.0001,").append(sie5Dhbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie5Rtree.getSelectQueryResultCount(1)).append(",")
                .append(sie5Shbtree.getSelectQueryResultCount(1)).append(",")
                .append(sie5Sif.getSelectQueryResultCount(1)).append("\n");
        sb.append("0.001,").append(sie5Dhbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie5Rtree.getSelectQueryResultCount(2)).append(",")
                .append(sie5Shbtree.getSelectQueryResultCount(2)).append(",")
                .append(sie5Sif.getSelectQueryResultCount(2)).append("\n");

        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie5_select_query_result_count1.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        sb.setLength(0);
        sb.append("# sie5 select query result count 2 report\n");

        sb.append("radius, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("0.01,").append(sie5Dhbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie5Rtree.getSelectQueryResultCount(3)).append(",")
                .append(sie5Shbtree.getSelectQueryResultCount(3)).append(",")
                .append(sie5Sif.getSelectQueryResultCount(3)).append("\n");
        sb.append("0.1,").append(sie5Dhbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie5Dhvbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie5Rtree.getSelectQueryResultCount(4)).append(",")
                .append(sie5Shbtree.getSelectQueryResultCount(4)).append(",")
                .append(sie5Sif.getSelectQueryResultCount(4)).append("\n");

        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie5_select_query_result_count2.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }
}
