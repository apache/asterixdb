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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

public class OperatorProfilerReportBuilder {

    private static final int INDEX_BUILD_OP_COUNT = 1;
    private static final int PIDX_SCAN_OP_COUNT = 1;
    private static final int WARM_UP_SELECT_QUERY_COUNT = 500;
    private static final int SELECT_QUERY_COUNT = 5000;
    private static final int JOIN_QUERY_COUNT = 200;
    private static final int JOIN_RADIUS_TYPE_COUNT = 4;
    private static final int SELECT_RADIUS_TYPE_COUNT = 5;
    private static final int IDX_JOIN_RADIUS_SKIP = JOIN_RADIUS_TYPE_COUNT - 1;
    private static final int IDX_SELECT_RADIUS_SKIP = SELECT_RADIUS_TYPE_COUNT - 1;
    private static final int IDX_INITIAL_JOIN_SKIP = INDEX_BUILD_OP_COUNT + PIDX_SCAN_OP_COUNT
            + WARM_UP_SELECT_QUERY_COUNT + SELECT_QUERY_COUNT;
    private static final int IDX_INITIAL_SELECT_SKIP = INDEX_BUILD_OP_COUNT + PIDX_SCAN_OP_COUNT
            + WARM_UP_SELECT_QUERY_COUNT;

    private static final int HYRACK_JOB_ELAPSED_TIME_FIELD = 2;
    private static final int OP_ELAPSED_TIME_FIELD = 4;
    private static final int OP_TASK_ID_FIELD = 2;
    private static final int OP_NAME_FIELD = 1;

    private String executionTimeFilePath = null;
    private BufferedReader brExecutionTime;
    private String line;
    private int lineNum;

    public OperatorProfilerReportBuilder(String executionTimeFilePath) {
        this.executionTimeFilePath = executionTimeFilePath;
    }

    public String getIdxNumber(boolean isJoin, int radiusIdx) throws Exception {
        openExecutionTimeFile();

        StringBuilder sb = new StringBuilder();
        int initialSkip = (isJoin ? IDX_INITIAL_JOIN_SKIP : IDX_INITIAL_SELECT_SKIP) + radiusIdx;
        int radiusSkip = isJoin ? IDX_JOIN_RADIUS_SKIP : IDX_SELECT_RADIUS_SKIP;
        BufferedReader br = brExecutionTime;
        int queryCount = isJoin ? JOIN_QUERY_COUNT / JOIN_RADIUS_TYPE_COUNT : SELECT_QUERY_COUNT
                / SELECT_RADIUS_TYPE_COUNT;
        lineNum = 0;
        JobStat jobStat = new JobStat();

        try {

            //initial skip
            int jobCount = 0;
            while ((line = br.readLine()) != null) {
                lineNum++;
                if (line.contains("TOTAL_HYRACKS_JOB")) {
                    jobCount++;
                    if (jobCount > initialSkip) {
                        break;
                    }
                }
            }

            //Reaching Here, line variable contains the first job to be counted
            for (int j = 0; j < queryCount; j++) {

                analyzeOperatorExecutionTime(jobStat, br);

                //radius skip
                jobCount = 0;
                while ((line = br.readLine()) != null) {
                    lineNum++;
                    if (line.contains("TOTAL_HYRACKS_JOB")) {
                        jobCount++;
                        if (jobCount > radiusSkip) {
                            break;
                        }
                    }
                }
            }

            //System.out.println("lineNum: " + lineNum);
            sb.append("TOTAL_HYRACKS_JOB," + (((double) jobStat.getHyracksJobTimeSum()) / jobStat.getHyracksJobCount())
                    + "," + jobStat.getHyracksJobTimeSum() + "," + jobStat.getHyracksJobCount() + "\n");
            sb.append(jobStat.getOperatorsElapsedTimeAsString());
            return sb.toString();
        } finally {
            closeExecutionTimeFile();
        }
    }

    private void analyzeOperatorExecutionTime(JobStat jobStat, BufferedReader br) throws IOException {
        //the line argument contains TOTAL_HYRACKS_JOB string. eg.:
        //2015-11-04 19:13:08,003   TOTAL_HYRACKS_JOB a1_node1_JID:3_26202768 TOTAL_HYRACKS_JOB1446660788003    1066    1.066   1066    1.066
        String tokens[] = line.split("\t");

        if (Long.parseLong(tokens[HYRACK_JOB_ELAPSED_TIME_FIELD]) > 10000) {
            System.out.println("[" + lineNum + "] " + line);
        }

        jobStat.addHyracksJobTime(Long.parseLong(tokens[HYRACK_JOB_ELAPSED_TIME_FIELD]));

        while ((line = br.readLine()) != null) {
            lineNum++;

            if (line.isEmpty()) {
                break;
            }

            tokens = line.split("\t");
            if (line.contains("DISTRIBUTE_RESULT")) {
                jobStat.addDistributeResultTime(Long.parseLong(tokens[OP_ELAPSED_TIME_FIELD]));
                continue;
            }
            if (line.contains("EMPTY_TUPLE_SOURCE")) {
                continue;
            }

            if (line.contains("TXN_JOB_COMMIT")) {
                continue;
            }

            jobStat.updateOperatorTime(tokens[OP_TASK_ID_FIELD], tokens[OP_NAME_FIELD],
                    Long.parseLong(tokens[OP_ELAPSED_TIME_FIELD]));
        }

        jobStat.updateTaskForAvgWithSlowestTask();
    }

    protected void openExecutionTimeFile() throws IOException {
        brExecutionTime = new BufferedReader(new FileReader(executionTimeFilePath));
    }

    protected void closeExecutionTimeFile() throws IOException {
        if (brExecutionTime != null) {
            brExecutionTime.close();
        }
    }

    class JobStat {
        private long hyracksJobElapsedTimeSum;
        private int hyracksJobCount;
        private long distributeResultTimeSum;
        private Task taskForAvg;
        private HashMap<String, Task> taskId2TaskMap;

        public JobStat() {
            hyracksJobElapsedTimeSum = 0;
            hyracksJobCount = 0;
            distributeResultTimeSum = 0;
            taskForAvg = new Task("TaskForAvg");
            taskId2TaskMap = new HashMap<String, Task>();
        }

        public void reset() {
            hyracksJobElapsedTimeSum = 0;
            hyracksJobCount = 0;
            distributeResultTimeSum = 0;
            taskForAvg.reset();;
            taskId2TaskMap.clear();
        }

        public void addHyracksJobTime(long elapsedTime) {
            hyracksJobElapsedTimeSum += elapsedTime;
            hyracksJobCount++;
        }

        public void addDistributeResultTime(long elapsedTime) {
            distributeResultTimeSum += elapsedTime;
        }

        public long getDistributeResultTime() {
            return distributeResultTimeSum;
        }

        public long getHyracksJobTimeSum() {
            return hyracksJobElapsedTimeSum;
        }

        public int getHyracksJobCount() {
            return hyracksJobCount;
        }

        public void updateOperatorTime(String taskId, String operatorName, long elapsedTime) {
            Task task = taskId2TaskMap.get(taskId);
            if (task == null) {
                task = new Task(taskId);
                taskId2TaskMap.put(new String(taskId), task);
            }
            task.updateOperatorTime(operatorName, elapsedTime);
        }

        public void updateTaskForAvgWithSlowestTask() {
            Iterator<Entry<String, Task>> taskIter = taskId2TaskMap.entrySet().iterator();
            Task slowestTask = null;
            Task curTask;

            //get the slowest task
            while (taskIter.hasNext()) {
                curTask = taskIter.next().getValue();
                if (slowestTask == null) {
                    slowestTask = curTask;
                } else {
                    if (slowestTask.getElapsedTime() < curTask.getElapsedTime()) {
                        slowestTask = curTask;
                    }
                }
            }

            //update the TaskForAvg with the slowest one
            HashMap<String, SumCount> operator2SumCountMap = slowestTask.getOperator2SumCountMap();
            Iterator<Entry<String, SumCount>> operatorIter = operator2SumCountMap.entrySet().iterator();
            while (operatorIter.hasNext()) {
                Entry<String, SumCount> entry = operatorIter.next();
                SumCount sc = entry.getValue();
                taskForAvg.updateOperatorTime(entry.getKey(), sc.sum);
            }
            taskId2TaskMap.clear();
        }

        public String getOperatorsElapsedTimeAsString() {
            return "SUM_OF_OPERATORS," + (((double) taskForAvg.getElapsedTime()) / hyracksJobCount) + ","
                    + taskForAvg.getElapsedTime() + "," + hyracksJobCount + "\n"
                    + taskForAvg.getOperatorsElapsedTimeAsString() + "DISTRIBUTE_RESULT,"
                    + (((double) distributeResultTimeSum) / hyracksJobCount) + "," + distributeResultTimeSum + ","
                    + hyracksJobCount + "\n";
        }
    }

    class Task {
        private String taskId;
        private long elapsedTime;
        private HashMap<String, SumCount> operator2SumCountMap;

        public Task(String taskId) {
            this.taskId = new String(taskId);
            elapsedTime = 0;
            operator2SumCountMap = new HashMap<String, SumCount>();
        }

        @Override
        public int hashCode() {
            return taskId.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof Task)) {
                return false;
            }
            return ((Task) o).taskId == taskId;
        }

        public long getElapsedTime() {
            return elapsedTime;
        }

        public void updateOperatorTime(String operatorName, long elapsedTime) {
            SumCount sc = operator2SumCountMap.get(operatorName);
            if (sc == null) {
                sc = new SumCount();
                sc.sum = 0;
                sc.count = 0;
                operator2SumCountMap.put(new String(operatorName), sc);
            }
            sc.sum += elapsedTime;
            sc.count++;
            this.elapsedTime += elapsedTime;
        }

        public void reset() {
            elapsedTime = 0;
            operator2SumCountMap.clear();
        }

        public String getOperatorsElapsedTimeAsString() {
            StringBuilder sb = new StringBuilder();
            Iterator<Entry<String, SumCount>> iter = operator2SumCountMap.entrySet().iterator();
            while (iter.hasNext()) {
                Entry<String, SumCount> entry = iter.next();
                SumCount sc = entry.getValue();
                sb.append(entry.getKey()).append(",").append(((double) sc.sum) / sc.count).append(",").append(sc.sum)
                        .append(",").append(sc.count).append("\n");
            }
            return sb.toString();
        }

        public HashMap<String, SumCount> getOperator2SumCountMap() {
            return operator2SumCountMap;
        }
    }

    class SumCount {
        public long sum;
        public int count;
    }

}
