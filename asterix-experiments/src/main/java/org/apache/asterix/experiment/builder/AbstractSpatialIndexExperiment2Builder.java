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

package org.apache.asterix.experiment.builder;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.asterix.experiment.action.base.AbstractAction;
import org.apache.asterix.experiment.action.base.ParallelActionSet;
import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.AbstractRemoteExecutableAction;
import org.apache.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;
import org.apache.asterix.experiment.client.SpatialIndexExperiment2OrchestratorServer;
import org.apache.commons.lang3.StringUtils;

public abstract class AbstractSpatialIndexExperiment2Builder extends AbstractLSMBaseExperimentBuilder {

    private static final long DOMAIN_SIZE = (1L << 32);

    public static final long QUERY_BEGIN_ROUND = 6;

    private static int N_PARTITIONS = 16;

    private final int nIntervals;

    private final String dataGenOrchHost;

    private final int dataGenOrchPort;

    private final String queryGenOrchHost;

    private final int queryGenOrchPort;

    private final long queryGenDuration;

    protected final long dataInterval;

    protected final int nQueryRuns;

    protected final Random randGen;

    protected final boolean isIndexOnlyPlan;

    public AbstractSpatialIndexExperiment2Builder(String name, LSMExperimentSetRunnerConfig config,
            String clusterConfigFileName, String ingestFileName, String dgenFileName, boolean isIndexOnlyPlan) {
        super(name, config, clusterConfigFileName, ingestFileName, dgenFileName, null);
        nIntervals = config.getNIntervals();
        dataGenOrchHost = config.getOrchestratorHost();
        dataGenOrchPort = config.getOrchestratorPort();
        dataInterval = config.getDataInterval();
        queryGenOrchHost = config.getQueryOrchestratorHost();
        queryGenOrchPort = config.getQueryOrchestratorPort();
        queryGenDuration = config.getQueryDuration();
        this.nQueryRuns = config.getNQueryRuns();
        this.randGen = new Random();
        this.isIndexOnlyPlan = isIndexOnlyPlan;
    }

    @Override
    protected void doBuildDataGen(SequentialActionList seq, Map<String, List<String>> dgenPairs) throws Exception {
        int nDgens = 0;
        for (List<String> v : dgenPairs.values()) {
            nDgens += v.size();
        }
        final SpatialIndexExperiment2OrchestratorServer oServer = new SpatialIndexExperiment2OrchestratorServer(
                dataGenOrchPort, nDgens, nIntervals, queryGenOrchPort, nDgens /*for now, query gen uses the same node as data gen*/);

        seq.add(new AbstractAction() {

            @Override
            protected void doPerform() throws Exception {
                oServer.start();
            }
        });

        //prepare data gen runner and query gen runner
        //note that dgenPairs.keySet() are used as query gen runners' ip address.
        ParallelActionSet dataAndQueryGenActions = new ParallelActionSet();
        int partition = 0;
        for (String dgenHost : dgenPairs.keySet()) {
            final List<String> rcvrs = dgenPairs.get(dgenHost);
            final int p = partition;
            //prepare data gen
            dataAndQueryGenActions.add(new AbstractRemoteExecutableAction(dgenHost, username, sshKeyLocation) {

                @Override
                protected String getCommand() {
                    String ipPortPairs = StringUtils.join(rcvrs.iterator(), " ");
                    String binary = "JAVA_HOME=" + javaHomePath + " "
                            + localExperimentRoot.resolve("bin").resolve("datagenrunner").toString();
                    if (openStreetMapFilePath == null) {
                        return StringUtils.join(new String[] { binary, "-rcbi",
                                "" + recordCountPerBatchDuringIngestionOnly, "-rcbq",
                                "" + recordCountPerBatchDuringQuery, "-dsti", "" + dataGenSleepTimeDuringIngestionOnly,
                                "-dstq", "" + dataGenSleepTimeDuringQuery, "-si", "" + locationSampleInterval, "-p",
                                "" + p, "-di", "" + dataInterval, "-ni", "" + nIntervals, "-qd", "" + queryGenDuration,
                                "-oh", dataGenOrchHost, "-op", "" + dataGenOrchPort, ipPortPairs }, " ");
                    } else {
                        return StringUtils.join(new String[] { binary, "-rcbi",
                                "" + recordCountPerBatchDuringIngestionOnly, "-rcbq",
                                "" + recordCountPerBatchDuringQuery, "-dsti", "" + dataGenSleepTimeDuringIngestionOnly,
                                "-dstq", "" + dataGenSleepTimeDuringQuery, "-si", "" + locationSampleInterval, "-of",
                                openStreetMapFilePath, "-p", "" + p, "-di", "" + dataInterval, "-ni", "" + nIntervals,
                                "-qd", "" + queryGenDuration, "-oh", dataGenOrchHost, "-op", "" + dataGenOrchPort,
                                ipPortPairs }, " ");
                    }
                }
            });

            //prepare query gen
            dataAndQueryGenActions.add(new AbstractRemoteExecutableAction(dgenHost, username, sshKeyLocation) {

                @Override
                protected String getCommand() {
                    String ipPortPairs = StringUtils.join(rcvrs.iterator(), " ");
                    String binary = "JAVA_HOME=" + javaHomePath + " "
                            + localExperimentRoot.resolve("bin").resolve("querygenrunner").toString();
                    if (openStreetMapFilePath == null) {
                        if (isIndexOnlyPlan) {
                            return StringUtils.join(new String[] { binary, "-iop", "-p", "" + p, "-qd",
                                    "" + queryGenDuration, "-qoh", "" + queryGenOrchHost, "-qop",
                                    "" + queryGenOrchPort, "-rh", restHost, "-rp", "" + restPort }, " ");
                        } else {
                            return StringUtils.join(new String[] { binary, "-p", "" + p, "-qd", "" + queryGenDuration,
                                    "-qoh", "" + queryGenOrchHost, "-qop", "" + queryGenOrchPort, "-rh", restHost,
                                    "-rp", "" + restPort }, " ");
                        }
                    } else {
                        if (isIndexOnlyPlan) {
                            return StringUtils.join(new String[] { binary, "-iop", "-of", openStreetMapFilePath, "-p",
                                    "" + p, "-qd", "" + queryGenDuration, "-qoh", "" + queryGenOrchHost, "-qop",
                                    "" + queryGenOrchPort, "-rh", restHost, "-rp", "" + restPort }, " ");
                        } else {
                            return StringUtils.join(new String[] { binary, "-of", openStreetMapFilePath, "-p", "" + p,
                                    "-qd", "" + queryGenDuration, "-qoh", "" + queryGenOrchHost, "-qop",
                                    "" + queryGenOrchPort, "-rh", restHost, "-rp", "" + restPort }, " ");
                        }
                    }
                }
            });

            partition += rcvrs.size();
        }
        seq.add(dataAndQueryGenActions);

        // wait until all dgen / queries are done
        seq.add(new AbstractAction() {

            @Override
            protected void doPerform() throws Exception {
                oServer.awaitFinished();
            }
        });
    }
}
