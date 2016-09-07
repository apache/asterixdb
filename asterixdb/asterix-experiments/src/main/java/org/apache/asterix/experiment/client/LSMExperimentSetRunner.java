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

package org.apache.asterix.experiment.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.builder.AbstractExperimentBuilder;
import org.apache.asterix.experiment.builder.PerfTestAggBuilder;
import org.apache.asterix.experiment.builder.PresetClusterPerfBuilder;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class LSMExperimentSetRunner {

    private static final Logger LOGGER = Logger.getLogger(LSMExperimentSetRunner.class.getName());

    public static class LSMExperimentSetRunnerConfig {

        private final String logDirSuffix;

        private final int nQueryRuns;

        public LSMExperimentSetRunnerConfig(String logDirSuffix, int nQueryRuns) {
            this.logDirSuffix = logDirSuffix;
            this.nQueryRuns = nQueryRuns;
        }

        public String getLogDirSuffix() {
            return logDirSuffix;
        }

        public int getNQueryRuns() {
            return nQueryRuns;
        }

        @Option(name = "-rh", aliases = "--rest-host", usage = "Asterix REST API host address", required = true, metaVar = "HOST")
        private String restHost;

        public String getRESTHost() {
            return restHost;
        }

        @Option(name = "-rp", aliases = "--rest-port", usage = "Asterix REST API port", required = true, metaVar = "PORT")
        private int restPort;

        public int getRESTPort() {
            return restPort;
        }

        @Option(name = "-mh", aliases = "--managix-home", usage = "Path to MANAGIX_HOME directory", required = true, metaVar = "MGXHOME")
        private String managixHome;

        public String getManagixHome() {
            return managixHome;
        }

        @Option(name = "-jh", aliases = "--java-home", usage = "Path to JAVA_HOME directory", required = true, metaVar = "JAVAHOME")
        private String javaHome;

        public String getJavaHome() {
            return javaHome;
        }

        @Option(name = "-ler", aliases = "--local-experiment-root", usage = "Path to the local LSM experiment root directory", required = true, metaVar = "LOCALEXPROOT")
        private String localExperimentRoot;

        public String getLocalExperimentRoot() {
            return localExperimentRoot;
        }

        @Option(name = "-u", aliases = "--username", usage = "Username to use for SSH/SCP", required = true, metaVar = "UNAME")
        private String username;

        public String getUsername() {
            return username;
        }

        @Option(name = "-k", aliases = "--key", usage = "SSH key location", metaVar = "SSHKEY")
        private String sshKeyLocation;

        public String getSSHKeyLocation() {
            return sshKeyLocation;
        }

        @Option(name = "-d", aliases = "--datagen-duration", usage = "Data generation duration in seconds", metaVar = "DATAGENDURATION")
        private int duration;

        public int getDuration() {
            return duration;
        }

        @Option(name = "-qd", aliases = "--querygen-duration", usage = "Query generation duration in seconds", metaVar = "QUERYGENDURATION")
        private int queryDuration;

        public int getQueryDuration() {
            return queryDuration;
        }

        @Option(name = "-regex", aliases = "--regex", usage = "Regular expression used to match experiment names", metaVar = "REGEXP")
        private String regex;

        public String getRegex() {
            return regex;
        }

        @Option(name = "-oh", aliases = "--orchestrator-host", usage = "The host address of THIS orchestrator")
        private String orchHost;

        public String getOrchestratorHost() {
            return orchHost;
        }

        @Option(name = "-op", aliases = "--orchestrator-port", usage = "The port to be used for the orchestrator server of THIS orchestrator")
        private int orchPort;

        public int getOrchestratorPort() {
            return orchPort;
        }

        @Option(name = "-qoh", aliases = "--query-orchestrator-host", usage = "The host address of query orchestrator")
        private String queryOrchHost;

        public String getQueryOrchestratorHost() {
            return queryOrchHost;
        }

        @Option(name = "-qop", aliases = "--query-orchestrator-port", usage = "The port to be used for the orchestrator server of query orchestrator")
        private int queryOrchPort;

        public int getQueryOrchestratorPort() {
            return queryOrchPort;
        }

        @Option(name = "-di", aliases = "--data-interval", usage = " Initial data interval to use when generating data for exp 7")
        private long dataInterval;

        public long getDataInterval() {
            return dataInterval;
        }

        @Option(name = "-ni", aliases = "--num-data-intervals", usage = "Number of data intervals to use when generating data for exp 7")
        private int numIntervals;

        public int getNIntervals() {
            return numIntervals;
        }

        @Option(name = "-sf", aliases = "--stat-file", usage = "Enable IO/CPU stats and place in specified file")
        private String statFile = null;

        public String getStatFile() {
            return statFile;
        }

        @Option(name = "-of", aliases = "--openstreetmap-filepath", usage = "The open street map gps point data file path")
        private String openStreetMapFilePath;

        public String getOpenStreetMapFilePath() {
            return openStreetMapFilePath;
        }

        @Option(name = "-si", aliases = "--location-sample-interval", usage = "Location sample interval from open street map point data")
        private int locationSampleInterval;

        public int getLocationSampleInterval() {
            return locationSampleInterval;
        }

        @Option(name = "-qsf", aliases = "--query-seed-filepath", usage = "The query seed file path")
        private String querySeedFilePath;

        public String getQuerySeedFilePath() {
            return querySeedFilePath;
        }

        @Option(name = "-rcbi", aliases = "--record-count-per-batch-during-ingestion-only", usage = "Record count per batch during ingestion only")
        private int recordCountPerBatchDuringIngestionOnly = 1000;

        public int getRecordCountPerBatchDuringIngestionOnly() {
            return recordCountPerBatchDuringIngestionOnly;
        }

        @Option(name = "-rcbq", aliases = "--record-count-per-batch-during-query", usage = "Record count per batch during query")
        private int recordCountPerBatchDuringQuery = 1000;

        public int getRecordCountPerBatchDuringQuery() {
            return recordCountPerBatchDuringQuery;
        }

        @Option(name = "-dsti", aliases = "--data-gen-sleep-time-during-ingestion-only", usage = "DataGen sleep time in milliseconds after every recordCountPerBatchDuringIngestionOnly records were sent")
        private long dataGenSleepTimeDuringIngestionOnly = 1;

        public long getDataGenSleepTimeDuringIngestionOnly() {
            return dataGenSleepTimeDuringIngestionOnly;
        }

        @Option(name = "-dstq", aliases = "--data-gen-sleep-time-during-query", usage = "DataGen sleep time in milliseconds after every recordCountPerBatchDuringQuery records were sent")
        private long dataGenSleepTimeDuringQuery = 1;

        public long getDataGenSleepTimeDuringQuery() {
            return dataGenSleepTimeDuringQuery;
        }
    }

    public static void main(String[] args) throws Exception {
        //        LogManager.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
        LSMExperimentSetRunnerConfig config = new LSMExperimentSetRunnerConfig(String.valueOf(System
                .currentTimeMillis()), 3);
        CmdLineParser clp = new CmdLineParser(config);
        try {
            clp.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            clp.printUsage(System.err);
            System.exit(1);
        }

        Collection<AbstractExperimentBuilder> suite = new ArrayList<>();

        /*
                suite.add(new Experiment7BBuilder(config));
                suite.add(new Experiment7DBuilder(config));
                suite.add(new Experiment7ABuilder(config));
                suite.add(new Experiment8DBuilder(config));
                suite.add(new Experiment8ABuilder(config));
                suite.add(new Experiment8BBuilder(config));
                suite.add(new Experiment9ABuilder(config));
                suite.add(new Experiment9DBuilder(config));
                suite.add(new Experiment9BBuilder(config));
                suite.add(new Experiment6ABuilder(config));
                suite.add(new Experiment6BBuilder(config));
                suite.add(new Experiment6CBuilder(config));
                suite.add(new Experiment2D1Builder(config));
                suite.add(new Experiment2D2Builder(config));
                suite.add(new Experiment2D4Builder(config));
                suite.add(new Experiment2D8Builder(config));
                suite.add(new Experiment2C1Builder(config));
                suite.add(new Experiment2C2Builder(config));
                suite.add(new Experiment2C4Builder(config));
                suite.add(new Experiment2C8Builder(config));
                suite.add(new Experiment2A1Builder(config));
                suite.add(new Experiment2A2Builder(config));
                suite.add(new Experiment2A4Builder(config));
                suite.add(new Experiment2A8Builder(config));
                suite.add(new Experiment2B1Builder(config));
                suite.add(new Experiment2B2Builder(config));
                suite.add(new Experiment2B4Builder(config));
                suite.add(new Experiment2B8Builder(config));
                suite.add(new Experiment1ABuilder(config));
                suite.add(new Experiment1BBuilder(config));
                suite.add(new Experiment1CBuilder(config));
                suite.add(new Experiment1DBuilder(config));
                suite.add(new Experiment1EBuilder(config));
                suite.add(new Experiment4ABuilder(config));
                suite.add(new Experiment4BBuilder(config));
                suite.add(new Experiment4CBuilder(config));
                suite.add(new Experiment4DBuilder(config));
                suite.add(new Experiment3ABuilder(config));
                suite.add(new Experiment3BBuilder(config));
                suite.add(new Experiment3CBuilder(config));
                suite.add(new Experiment3DBuilder(config));
                suite.add(new Experiment5ABuilder(config));
                suite.add(new Experiment5BBuilder(config));
                suite.add(new Experiment5CBuilder(config));
                suite.add(new Experiment5DBuilder(config));
        */
                suite.add(new PerfTestAggBuilder(config));
                suite.add(new PresetClusterPerfBuilder(config));

        Pattern p = config.getRegex() == null ? null : Pattern.compile(config.getRegex());

        SequentialActionList exps = new SequentialActionList();
        for (AbstractExperimentBuilder eb : suite) {
            if (p == null || p.matcher(eb.getName()).matches()) {
                exps.add(eb.build());
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Added " + eb.getName() + " to run list...");
                }
            }
        }
        exps.perform();
    }
}
