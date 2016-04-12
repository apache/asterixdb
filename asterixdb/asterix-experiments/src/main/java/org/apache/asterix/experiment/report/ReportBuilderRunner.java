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

public class ReportBuilderRunner {
    public static final boolean REPORT_SIE1 = false;
    public static final boolean REPORT_SIE2 = false;
    public static final boolean REPORT_SIE3 = false;
    public static final boolean REPORT_SIE4 = false;
    public static final boolean REPORT_SIE5 = true;
    public static final boolean REPORT_SIE3_PROFILE = false;

    public static void main(String[] args) throws Exception {

        if (REPORT_SIE1) {
            SIE1ReportBuilderRunner sie1 = new SIE1ReportBuilderRunner();
            sie1.generateSIE1IPS();
            //        sie1.generateInstantaneousInsertPS();
            sie1.generateIndexSize();
            sie1.generateGanttInstantaneousInsertPS();
            sie1.generateAccumulatedInsertPS();
        }

        if (REPORT_SIE2) {
            SIE2ReportBuilderRunner sie2 = new SIE2ReportBuilderRunner();
            sie2.generateOverallInsertPS();
            sie2.generateAccumulatedInsertPS();
            sie2.generateQueryPS();
            sie2.generateAverageQueryResultCount();
            sie2.generateAverageQueryResponseTime();
            sie2.generateInstantaneousInsertPS();
            sie2.generateGanttInstantaneousInsertPS();
            sie2.generateSelectQueryResponseTime();
            sie2.generateSelectQueryResultCount();
        }

        if (REPORT_SIE3) {
            SIE3ReportBuilderRunner sie3 = new SIE3ReportBuilderRunner();
            sie3.generateIndexCreationTime();
            sie3.generateIndexSize();
            sie3.generateSelectQueryResponseTime();
            sie3.generateJoinQueryResponseTime();
            sie3.generateSelectQueryResultCount();
            sie3.generateJoinQueryResultCount();

            //            sie3.generateSelectQueryProfiledSidxSearchTime();
            //            sie3.generateSelectQueryProfiledPidxSearchTime();
            //            sie3.generateJoinQueryProfiledSidxSearchTime();
            //            sie3.generateJoinQueryProfiledPidxSearchTime();
            //            sie3.generateJoinQueryProfiledSeedPidxSearchTime();
            //            sie3.generateSelectQueryProfiledSidxCacheMiss();
            //            sie3.generateSelectQueryProfiledPidxCacheMiss();
            //            sie3.generateJoinQueryProfiledSidxCacheMiss();
            //            sie3.generateJoinQueryProfiledPidxCacheMiss();
            //            sie3.generateJoinQueryProfiledSeedPidxCacheMiss();
            //            sie3.generateSelectQueryProfiledFalsePositive();
            //            sie3.generateJoinQueryProfiledFalsePositive();
        }

        if (REPORT_SIE4) {
            SIE4ReportBuilderRunner sie4 = new SIE4ReportBuilderRunner();
            sie4.generateIndexCreationTime();
            sie4.generateIndexSize();
            sie4.generateSelectQueryResponseTime();
            sie4.generateJoinQueryResponseTime();
            sie4.generateSelectQueryResultCount();
            sie4.generateJoinQueryResultCount();
        }

        if (REPORT_SIE5) {
            SIE5ReportBuilderRunner sie5 = new SIE5ReportBuilderRunner();
            sie5.generateOverallInsertPS();
            sie5.generateAccumulatedInsertPS();
            sie5.generateQueryPS();
            sie5.generateAverageQueryResultCount();
            sie5.generateAverageQueryResponseTime();
            sie5.generateInstantaneousInsertPS();
            sie5.generateGanttInstantaneousInsertPS();
            sie5.generateSelectQueryResponseTime();
            sie5.generateSelectQueryResultCount();
        }

        if (REPORT_SIE3_PROFILE) {
            String executionTimeFilePath[] = new String[5];
            executionTimeFilePath[0] = "/Users/kisskys/workspace/asterix_master/resultLog/Mem3g-Disk4g-part4-Lsev-Jvm5g-Lock6g/profile-exp3/SpatialIndexExperiment3Dhbtree/logs/executionTime-130.149.249.52.txt";
            executionTimeFilePath[1] = "/Users/kisskys/workspace/asterix_master/resultLog/Mem3g-Disk4g-part4-Lsev-Jvm5g-Lock6g/profile-exp3/SpatialIndexExperiment3Dhvbtree/logs/executionTime-130.149.249.52.txt";
            executionTimeFilePath[2] = "/Users/kisskys/workspace/asterix_master/resultLog/Mem3g-Disk4g-part4-Lsev-Jvm5g-Lock6g/profile-exp3/SpatialIndexExperiment3Rtree/logs/executionTime-130.149.249.52.txt";
            executionTimeFilePath[3] = "/Users/kisskys/workspace/asterix_master/resultLog/Mem3g-Disk4g-part4-Lsev-Jvm5g-Lock6g/profile-exp3/SpatialIndexExperiment3Shbtree/logs/executionTime-130.149.249.52.txt";
            executionTimeFilePath[4] = "/Users/kisskys/workspace/asterix_master/resultLog/Mem3g-Disk4g-part4-Lsev-Jvm5g-Lock6g/profile-exp3/SpatialIndexExperiment3Sif/logs/executionTime-130.149.249.52.txt";

            for (int i = 0; i < 5; i++) {
                String filePath = executionTimeFilePath[i];
                OperatorProfilerReportBuilder oprb = new OperatorProfilerReportBuilder(filePath);
                System.out.println("--------  " + i + " ----------\n");
                System.out.println(oprb.getIdxNumber(false, 0));
                System.out.println(oprb.getIdxNumber(false, 1));
                System.out.println(oprb.getIdxNumber(false, 2));
                System.out.println(oprb.getIdxNumber(false, 3));
                System.out.println(oprb.getIdxNumber(false, 4));
                System.out.println(oprb.getIdxNumber(true, 0));
                System.out.println(oprb.getIdxNumber(true, 1));
                System.out.println(oprb.getIdxNumber(true, 2));
                System.out.println(oprb.getIdxNumber(true, 3));
            }

        }

    }
}
