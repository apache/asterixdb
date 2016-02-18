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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.experiment.action.base.ParallelActionSet;
import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.AbstractRemoteExecutableAction;
import org.apache.asterix.experiment.action.derived.ManagixActions.CreateAsterixManagixAction;
import org.apache.asterix.experiment.action.derived.ManagixActions.DeleteAsterixManagixAction;
import org.apache.asterix.experiment.action.derived.ManagixActions.LogAsterixManagixAction;
import org.apache.asterix.experiment.action.derived.ManagixActions.StopAsterixManagixAction;
import org.apache.asterix.experiment.action.derived.RemoteAsterixDriverKill;
import org.apache.asterix.experiment.action.derived.RunAQLFileAction;
import org.apache.asterix.experiment.action.derived.SleepAction;
import org.apache.asterix.experiment.action.derived.TimedAction;
import org.apache.asterix.experiment.client.LSMExperimentConstants;
import org.apache.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * This class is used to create experiments for spatial index static data evaluation, that is, no ingestion is involved.
 * Also, there is no orchestration server involved in this experiment builder.
 */
public abstract class AbstractSpatialIndexExperiment3PIdxLoadBuilder extends AbstractExperimentBuilder {

    private static final String ASTERIX_INSTANCE_NAME = "a1";

    private final String logDirSuffix;

    protected final HttpClient httpClient;

    protected final String restHost;

    protected final int restPort;

    private final String managixHomePath;

    protected final String javaHomePath;

    protected final Path localExperimentRoot;

    protected final String username;

    protected final String sshKeyLocation;

    private final int duration;

    private final String clusterConfigFileName;

    private final String ingestFileName;

    protected final String dgenFileName;

    private final String countFileName;

    private final String statFile;

    protected final SequentialActionList lsAction;

    protected final String openStreetMapFilePath;

    protected final int locationSampleInterval;

    protected final String loadAQLFilePath;

    public AbstractSpatialIndexExperiment3PIdxLoadBuilder(String name, LSMExperimentSetRunnerConfig config,
            String clusterConfigFileName, String ingestFileName, String dgenFileName, String countFileName,
            String loadAQLFileName) {
        super(name);
        this.logDirSuffix = config.getLogDirSuffix();
        this.httpClient = new DefaultHttpClient();
        this.restHost = config.getRESTHost();
        this.restPort = config.getRESTPort();
        this.managixHomePath = config.getManagixHome();
        this.javaHomePath = config.getJavaHome();
        this.localExperimentRoot = Paths.get(config.getLocalExperimentRoot());
        this.username = config.getUsername();
        this.sshKeyLocation = config.getSSHKeyLocation();
        this.duration = config.getDuration();
        this.clusterConfigFileName = clusterConfigFileName;
        this.ingestFileName = ingestFileName;
        this.dgenFileName = dgenFileName;
        this.countFileName = countFileName;
        this.statFile = config.getStatFile();
        this.lsAction = new SequentialActionList();
        this.openStreetMapFilePath = config.getOpenStreetMapFilePath();
        this.locationSampleInterval = config.getLocationSampleInterval();
        this.loadAQLFilePath = loadAQLFileName;
    }

    protected abstract void doBuildDDL(SequentialActionList seq);

    protected void doPost(SequentialActionList seq) {
    }

    protected void doBuildDataGen(SequentialActionList seq, final Map<String, List<String>> dgenPairs) throws Exception {
    }

    @Override
    protected void doBuild(Experiment e) throws Exception {
        SequentialActionList execs = new SequentialActionList();

        String clusterConfigPath = localExperimentRoot.resolve(LSMExperimentConstants.CONFIG_DIR)
                .resolve(clusterConfigFileName).toString();
        String asterixConfigPath = localExperimentRoot.resolve(LSMExperimentConstants.CONFIG_DIR)
                .resolve(LSMExperimentConstants.ASTERIX_CONFIGURATION).toString();

        //stop/delete/create instance
        execs.add(new StopAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));
        execs.add(new DeleteAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));
        execs.add(new SleepAction(30000));
        execs.add(new CreateAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME, clusterConfigPath,
                asterixConfigPath));

        //ddl statements
        execs.add(new SleepAction(15000));
        // TODO: implement retry handler
        execs.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve(LSMExperimentConstants.BASE_TYPES)));
        doBuildDDL(execs);

        //prepare io state action in NC node(s)
        Map<String, List<String>> dgenPairs = readDatagenPairs(localExperimentRoot.resolve(
                LSMExperimentConstants.DGEN_DIR).resolve(dgenFileName));
        final Set<String> ncHosts = new HashSet<>();
        for (List<String> ncHostList : dgenPairs.values()) {
            for (String ncHost : ncHostList) {
                ncHosts.add(ncHost.split(":")[0]);
            }
        }
        if (statFile != null) {
            ParallelActionSet ioCountActions = new ParallelActionSet();
            for (String ncHost : ncHosts) {
                ioCountActions.add(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {

                    @Override
                    protected String getCommand() {
                        String cmd = "screen -d -m sh -c \"sar -b -u 1 > " + statFile + "\"";
                        return cmd;
                    }
                });
            }
            execs.add(ioCountActions);
        }

        //prepare post ls action
        SequentialActionList postLSAction = new SequentialActionList();
        File file = new File(clusterConfigPath);
        JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
        Unmarshaller unmarshaller = ctx.createUnmarshaller();
        final Cluster cluster = (Cluster) unmarshaller.unmarshal(file);
        String[] storageRoots = cluster.getIodevices().split(",");
        for (String ncHost : ncHosts) {
            for (final String sRoot : storageRoots) {
                lsAction.add(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {
                    @Override
                    protected String getCommand() {
                        return "ls -Rl " + sRoot;
                    }
                });
                postLSAction.add(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {
                    @Override
                    protected String getCommand() {
                        return "ls -Rl " + sRoot;
                    }
                });

            }
        }

        //---------- main experiment body begins -----------

        //load data into pidx 
        execs.add(new TimedAction(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve(loadAQLFilePath))));

        //---------- main experiment body ends -----------

        //kill io state action
        if (statFile != null) {
            ParallelActionSet ioCountKillActions = new ParallelActionSet();
            for (String ncHost : ncHosts) {
                ioCountKillActions.add(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {

                    @Override
                    protected String getCommand() {
                        String cmd = "screen -X -S `screen -list | grep Detached | awk '{print $1}'` quit";
                        return cmd;
                    }
                });
            }
            execs.add(ioCountKillActions);
        }

        //total record count
        execs.add(new SleepAction(10000));
        if (countFileName != null) {
            execs.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                    LSMExperimentConstants.AQL_DIR).resolve(countFileName)));
        }

        //add ls action
        execs.add(postLSAction);

        //kill asterix cc and nc
        ParallelActionSet killCmds = new ParallelActionSet();
        for (String ncHost : ncHosts) {
            killCmds.add(new RemoteAsterixDriverKill(ncHost, username, sshKeyLocation));
        }
        killCmds.add(new RemoteAsterixDriverKill(restHost, username, sshKeyLocation));
        execs.add(killCmds);

        //stop asterix instance
        execs.add(new StopAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));

        //prepare to collect io state by putting the state file into asterix log dir
        if (statFile != null) {
            ParallelActionSet collectIOActions = new ParallelActionSet();
            for (String ncHost : ncHosts) {
                collectIOActions.add(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {

                    @Override
                    protected String getCommand() {
                        String cmd = "cp " + statFile + " " + cluster.getLogDir();
                        return cmd;
                    }
                });
            }
            execs.add(collectIOActions);
        }

        //collect cc and nc logs
        execs.add(new LogAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME, localExperimentRoot
                .resolve(LSMExperimentConstants.LOG_DIR + "-" + logDirSuffix).resolve(getName()).toString()));

        e.addBody(execs);
    }

    protected Map<String, List<String>> readDatagenPairs(Path p) throws IOException {
        Map<String, List<String>> dgenPairs = new HashMap<>();
        Scanner s = new Scanner(p, StandardCharsets.UTF_8.name());
        try {
            while (s.hasNextLine()) {
                String line = s.nextLine();
                String[] pair = line.split("\\s+");
                List<String> vals = dgenPairs.get(pair[0]);
                if (vals == null) {
                    vals = new ArrayList<>();
                    dgenPairs.put(pair[0], vals);
                }
                vals.add(pair[1]);
            }
        } finally {
            s.close();
        }
        return dgenPairs;
    }
}
