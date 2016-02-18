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

public class SIE1ReportBuilderRunner {
    String expHomePath = "/Users/kisskys/workspace/asterix_master/resultLog/MemBuf3g-DiskBuf3g-Lsev-Jvm7g-Lock0g/exp1/";
    String runLogFileName = "run-exp1.log";
    String outputFilePath = "/Users/kisskys/workspace/asterix_master/resultLog/MemBuf3g-DiskBuf3g-Lsev-Jvm7g-Lock0g/result-report/";

    SIE1ReportBuilder sie1ADhbtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1ADhbtree",
            runLogFileName);
    SIE1ReportBuilder sie1ADhvbtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1ADhvbtree",
            runLogFileName);
    SIE1ReportBuilder sie1ARtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1ARtree", runLogFileName);
    SIE1ReportBuilder sie1AShbtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1AShbtree",
            runLogFileName);
    SIE1ReportBuilder sie1ASif = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1ASif", runLogFileName);

    SIE1ReportBuilder sie1BDhbtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1BDhbtree",
            runLogFileName);
    SIE1ReportBuilder sie1BDhvbtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1BDhvbtree",
            runLogFileName);
    SIE1ReportBuilder sie1BRtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1BRtree", runLogFileName);
    SIE1ReportBuilder sie1BShbtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1BShbtree",
            runLogFileName);
    SIE1ReportBuilder sie1BSif = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1BSif", runLogFileName);

    SIE1ReportBuilder sie1CDhbtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1CDhbtree",
            runLogFileName);
    SIE1ReportBuilder sie1CDhvbtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1CDhvbtree",
            runLogFileName);
    SIE1ReportBuilder sie1CRtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1CRtree", runLogFileName);
    SIE1ReportBuilder sie1CShbtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1CShbtree",
            runLogFileName);
    SIE1ReportBuilder sie1CSif = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1CSif", runLogFileName);

    SIE1ReportBuilder sie1DDhbtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1DDhbtree",
            runLogFileName);
    SIE1ReportBuilder sie1DDhvbtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1DDhvbtree",
            runLogFileName);
    SIE1ReportBuilder sie1DRtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1DRtree", runLogFileName);
    SIE1ReportBuilder sie1DShbtree = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1DShbtree",
            runLogFileName);
    SIE1ReportBuilder sie1DSif = new SIE1ReportBuilder(expHomePath, "SpatialIndexExperiment1DSif", runLogFileName);

    StringBuilder sb = new StringBuilder();

    /**
     * generate sie1_ips.txt
     */
    public void generateSIE1IPS() throws Exception {
        int minutes = 60;
        sb.setLength(0);
        sb.append("# sie1 ips(inserts per second) report\n");
        sb.append("# number of nodes, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("1,").append(sie1ADhbtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1ADhvbtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1ARtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1AShbtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1ASif.getOverallInsertPS(minutes)).append("\n");

        sb.append("2,").append(sie1BDhbtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1BDhvbtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1BRtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1BShbtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1BSif.getOverallInsertPS(minutes)).append("\n");

        sb.append("4,").append(sie1CDhbtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1CDhvbtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1CRtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1CShbtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1CSif.getOverallInsertPS(minutes)).append("\n");

        sb.append("8,").append(sie1DDhbtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1DDhvbtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1DRtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1DShbtree.getOverallInsertPS(minutes)).append(",")
                .append(sie1DSif.getOverallInsertPS(minutes)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie1_ips.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    /**
     * generate sie1_accumulated_insert_ps.txt
     */
    public void generateAccumulatedInsertPS() throws Exception {
        int targetRound = 721; //(3600 seconds / 5seconds) + 1
        int roundInterval = 5;

        ArrayList<Long> ipsListDhbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListDhvbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListRtree = new ArrayList<Long>();
        ArrayList<Long> ipsListShbtree = new ArrayList<Long>();
        ArrayList<Long> ipsListSif = new ArrayList<Long>();
        sie1DDhbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListDhbtree);
        sie1DDhvbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListDhvbtree);
        sie1DRtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListRtree);
        sie1DShbtree.getAllNodesAccumulatedInsertPS(targetRound, ipsListShbtree);
        sie1DSif.getAllNodesAccumulatedInsertPS(targetRound, ipsListSif);

        sb.setLength(0);
        sb.append("# sie1 accumulated inserts per second report\n");
        sb.append("# time, dhbtree, dhvbtree, rtree, shbtree, sif\n");

        for (int i = 0; i < targetRound; i++) {
            sb.append("" + (i * roundInterval) + "," + ipsListDhbtree.get(i) + "," + ipsListDhvbtree.get(i) + ","
                    + ipsListRtree.get(i) + "," + ipsListShbtree.get(i) + "," + ipsListSif.get(i) + "\n");
        }
        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie1_accumulated_insert_ps.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        ipsListDhbtree.clear();
        ipsListDhvbtree.clear();
        ipsListRtree.clear();
        ipsListShbtree.clear();
        ipsListSif.clear();
    }

    public void generateInstantaneousInsertPS() throws Exception {
        int nodeCount = 8;
        for (int i = 0; i < nodeCount; i++) {
            sb.setLength(0);
            sb.append("# sie1 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie1DDhbtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie1_8nodes_instantaneous_insert_ps_dhbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < nodeCount; i++) {
            sb.setLength(0);
            sb.append("# sie1 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie1DDhvbtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie1_8nodes_instantaneous_insert_ps_dhvbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < nodeCount; i++) {
            sb.setLength(0);
            sb.append("# sie1 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie1DRtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie1_8nodes_instantaneous_insert_ps_rtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < nodeCount; i++) {
            sb.setLength(0);
            sb.append("# sie1 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie1DShbtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie1_8nodes_instantaneous_insert_ps_shbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < nodeCount; i++) {
            sb.setLength(0);
            sb.append("# sie1 8nodes(8 dataGen) instantaneous inserts per second report\n");
            sb.append(sie1DSif.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie1_8nodes_instantaneous_insert_ps_sif_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
    }

    public void generateIndexSize() throws Exception {
        sb.setLength(0);
        sb.append("# sie1 index size report\n");

        sb.append("# number of nodes, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("1,").append(sie1ADhbtree.getIndexSize("Tweets_idx_dhbtreeLocation/device_id")).append(",")
                .append(sie1ADhvbtree.getIndexSize("Tweets_idx_dhvbtreeLocation/device_id")).append(",")
                .append(sie1ARtree.getIndexSize("Tweets_idx_rtreeLocation/device_id")).append(",")
                .append(sie1AShbtree.getIndexSize("Tweets_idx_shbtreeLocation/device_id")).append(",")
                .append(sie1ASif.getIndexSize("Tweets_idx_sifLocation/device_id")).append(",")
                .append(sie1ASif.getIndexSize("Tweets_idx_Tweets/device_id")).append("\n");
        sb.append("2,").append(sie1BDhbtree.getIndexSize("Tweets_idx_dhbtreeLocation/device_id")).append(",")
                .append(sie1BDhvbtree.getIndexSize("Tweets_idx_dhvbtreeLocation/device_id")).append(",")
                .append(sie1BRtree.getIndexSize("Tweets_idx_rtreeLocation/device_id")).append(",")
                .append(sie1BShbtree.getIndexSize("Tweets_idx_shbtreeLocation/device_id")).append(",")
                .append(sie1BSif.getIndexSize("Tweets_idx_sifLocation/device_id")).append(",")
                .append(sie1BSif.getIndexSize("Tweets_idx_Tweets/device_id")).append("\n");
        sb.append("4,").append(sie1CDhbtree.getIndexSize("Tweets_idx_dhbtreeLocation/device_id")).append(",")
                .append(sie1CDhvbtree.getIndexSize("Tweets_idx_dhvbtreeLocation/device_id")).append(",")
                .append(sie1CRtree.getIndexSize("Tweets_idx_rtreeLocation/device_id")).append(",")
                .append(sie1CShbtree.getIndexSize("Tweets_idx_shbtreeLocation/device_id")).append(",")
                .append(sie1CSif.getIndexSize("Tweets_idx_sifLocation/device_id")).append(",")
                .append(sie1CSif.getIndexSize("Tweets_idx_Tweets/device_id")).append("\n");
        sb.append("8,").append(sie1DDhbtree.getIndexSize("Tweets_idx_dhbtreeLocation/device_id")).append(",")
                .append(sie1DDhvbtree.getIndexSize("Tweets_idx_dhvbtreeLocation/device_id")).append(",")
                .append(sie1DRtree.getIndexSize("Tweets_idx_rtreeLocation/device_id")).append(",")
                .append(sie1DShbtree.getIndexSize("Tweets_idx_shbtreeLocation/device_id")).append(",")
                .append(sie1DSif.getIndexSize("Tweets_idx_sifLocation/device_id")).append(",")
                .append(sie1DSif.getIndexSize("Tweets_idx_Tweets/device_id")).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie1_index_size.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

    public void generateGanttInstantaneousInsertPS() throws Exception {

        SIE1ReportBuilder dhbtree = sie1DDhbtree;
        SIE1ReportBuilder dhvbtree = sie1DDhvbtree;
        SIE1ReportBuilder rtree = sie1DRtree;
        SIE1ReportBuilder shbtree = sie1DShbtree;
        SIE1ReportBuilder sif = sie1DSif;
        String sie1Type = "D";
        String logDirPrefix = "";

        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie1 1node(1 dataGen) instantaneous inserts per second report\n");
            sb.append(dhbtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie1_gantt_1node_instantaneous_insert_ps_dhbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie1 1node(1 dataGen) instantaneous inserts per second report\n");
            sb.append(dhvbtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie1_gantt_1node_instantaneous_insert_ps_dhvbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie1 1node(1 dataGen) instantaneous inserts per second report\n");
            sb.append(rtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie1_gantt_1node_instantaneous_insert_ps_rtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie1 1node(1 dataGen) instantaneous inserts per second report\n");
            sb.append(shbtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie1_gantt_1node_instantaneous_insert_ps_shbtree_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie1 1node(1 dataGen) instantaneous inserts per second report\n");
            sb.append(sif.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                    + "sie1_gantt_1node_instantaneous_insert_ps_sif_gen" + i + ".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }

        long dataGenStartTime = dhbtree.getDataGenStartTimeStamp();
        NCLogReportBuilder ncLogReportBuilder = new NCLogReportBuilder(expHomePath + "SpatialIndexExperiment1"
                + sie1Type + "Dhbtree/" + logDirPrefix + "logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        FileOutputStream fos = ReportBuilderHelper.openOutputFile(outputFilePath
                + "sie1_gantt_1node_flush_merge_dhbtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        dataGenStartTime = dhvbtree.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(expHomePath + "SpatialIndexExperiment1" + sie1Type + "Dhvbtree/"
                + logDirPrefix + "logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie1_gantt_1node_flush_merge_dhvbtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        dataGenStartTime = rtree.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(expHomePath + "SpatialIndexExperiment1" + sie1Type + "Rtree/"
                + logDirPrefix + "logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie1_gantt_1node_flush_merge_rtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        dataGenStartTime = shbtree.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(expHomePath + "SpatialIndexExperiment1" + sie1Type + "Shbtree/"
                + logDirPrefix + "logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie1_gantt_1node_flush_merge_shbtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);

        dataGenStartTime = sif.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(expHomePath + "SpatialIndexExperiment1" + sie1Type + "Sif/"
                + logDirPrefix + "logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(outputFilePath + "sie1_gantt_1node_flush_merge_sif.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }

}
