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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.Inet4Address;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.experiment.action.base.IAction;
import org.apache.asterix.experiment.action.base.ParallelActionSet;
import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.AbstractRemoteExecutableAction;
import org.apache.asterix.experiment.action.derived.CloseOutputStreamAction;
import org.apache.asterix.experiment.action.derived.ManagixActions.LogAsterixManagixAction;
import org.apache.asterix.experiment.action.derived.ManagixActions.StartAsterixManagixAction;
import org.apache.asterix.experiment.action.derived.ManagixActions.StopAsterixManagixAction;
import org.apache.asterix.experiment.action.derived.RemoteAsterixDriverKill;
import org.apache.asterix.experiment.action.derived.RunAQLFileAction;
import org.apache.asterix.experiment.action.derived.RunAQLStringAction;
import org.apache.asterix.experiment.action.derived.SleepAction;
import org.apache.asterix.experiment.action.derived.TimedAction;
import org.apache.asterix.experiment.client.LSMExperimentConstants;
import org.apache.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.hyracks.api.util.ExperimentProfilerUtils;

/**
 * This class is used to create experiments for spatial index static data evaluation, that is, no ingestion is involved.
 * Also, there is no orchestration server involved in this experiment builder.
 */
public abstract class AbstractSpatialIndexExperiment3SIdxCreateAndQueryBuilder extends AbstractExperimentBuilder {

    private static final boolean PROFILE_JOB_LAUCHING_OVERHEAD = false;

    private static final String ASTERIX_INSTANCE_NAME = "a1";
    private static final int SKIP_LINE_COUNT = 223;
    private static final int CACHE_WARM_UP_QUERY_COUNT = 500;
    private static final int SELECT_QUERY_COUNT = 5000;
    private static final int JOIN_QUERY_COUNT = 1000;

    private static final int JOIN_CANDIDATE_COUNT = 100;
    private static final int MAX_QUERY_SEED = 10000;

    private int querySeed = 0;

    private int queryCount = 0;

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

    protected final String createAQLFilePath;

    protected final String querySeedFilePath;

    private final float[] radiusType = new float[] { 0.00001f, 0.0001f, 0.001f, 0.01f, 0.1f };
    private int radiusIter = 0;
    private final Random randGen;
    private BufferedReader br;
    private final boolean isIndexOnlyPlan;
    private String outputFilePath;
    private FileOutputStream outputFos;

    public AbstractSpatialIndexExperiment3SIdxCreateAndQueryBuilder(String name, LSMExperimentSetRunnerConfig config,
            String clusterConfigFileName, String ingestFileName, String dgenFileName, String countFileName,
            String createAQLFileName, boolean isIndexOnlyPlan) {
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
        this.createAQLFilePath = createAQLFileName;
        this.querySeedFilePath = config.getQuerySeedFilePath();
        this.randGen = new Random();
        this.isIndexOnlyPlan = isIndexOnlyPlan;
    }

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

        //start asterix instance
        execs.add(new StartAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));
        execs.add(new SleepAction(30000));

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

        try {
            outputFilePath = openStreetMapFilePath.substring(0, openStreetMapFilePath.lastIndexOf(File.separator))
                    + File.separator + "QueryGenResult-" + getName() + "-"
                    + Inet4Address.getLocalHost().getHostAddress() + ".txt";
            outputFos = ExperimentProfilerUtils.openOutputFile(outputFilePath);
        } catch (Exception e1) {
            e1.printStackTrace();
            return;
        }

        //delete all existing secondary indexes if any
        execs.add(new RunAQLStringAction(httpClient, restHost, restPort,
                "use dataverse experiments; drop index Tweets.dhbtreeLocation;", outputFos));
        execs.add(new RunAQLStringAction(httpClient, restHost, restPort,
                "use dataverse experiments; drop index Tweets.dhvbtreeLocation;", outputFos));
        execs.add(new RunAQLStringAction(httpClient, restHost, restPort,
                "use dataverse experiments; drop index Tweets.rtreeLocation;", outputFos));
        execs.add(new RunAQLStringAction(httpClient, restHost, restPort,
                "use dataverse experiments; drop index Tweets.shbtreeLocation;", outputFos));
        execs.add(new RunAQLStringAction(httpClient, restHost, restPort,
                "use dataverse experiments; drop index Tweets.sifLocation;", outputFos));

        //create secondary index 
        execs.add(new TimedAction(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve(createAQLFilePath), outputFos), outputFos));

        //run count query for cleaning up OS buffer cache
        if (countFileName != null) {
            execs.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                    LSMExperimentConstants.AQL_DIR).resolve(countFileName), outputFos));
        }

        //run cache warm-up queries: run CACHE_WARM_UP_QUERY_COUNT select queries
        br = new BufferedReader(new FileReader(querySeedFilePath));
        radiusIter = 0;
        for (int i = 0; i < CACHE_WARM_UP_QUERY_COUNT; i++) {
            execs.add(getSelectQuery(isIndexOnlyPlan));
        }

        radiusIter = 0;
        //run queries for measurement: run SELECT_QUERY_COUNT select queries
        for (int i = 0; i < SELECT_QUERY_COUNT; i++) {
            execs.add(getSelectQuery(isIndexOnlyPlan));
        }

        radiusIter = 0;
        //run queries for measurement: run JOIN_QUERY_COUNT join queries
        for (int i = 0; i < JOIN_QUERY_COUNT; i++) {
            execs.add(getJoinQuery(isIndexOnlyPlan));
        }

        //---------- main experiment body ends -----------

        //kill io state action
        //        if (statFile != null) {
        //            ParallelActionSet ioCountKillActions = new ParallelActionSet();
        //            for (String ncHost : ncHosts) {
        //                ioCountKillActions.add(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {
        //
        //                    @Override
        //                    protected String getCommand() {
        //                        String cmd = "screen -X -S `screen -list | grep Detached | awk '{print $1}'` quit";
        //                        return cmd;
        //                    }
        //                });
        //            }
        //            execs.add(ioCountKillActions);
        //        }

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

        //collect profile information
        //        if (ExperimentProfiler.PROFILE_MODE) {
        //            if (!SpatialIndexProfiler.PROFILE_HOME_DIR.contentEquals(cluster.getLogDir())) {
        //                ParallelActionSet collectProfileInfo = new ParallelActionSet();
        //                for (String ncHost : ncHosts) {
        //                    collectProfileInfo.add(new AbstractRemoteExecutableAction(ncHost, username, sshKeyLocation) {
        //                        @Override
        //                        protected String getCommand() {
        //                            String cmd = "mv " + SpatialIndexProfiler.PROFILE_HOME_DIR + "*.txt " + cluster.getLogDir();
        //                            return cmd;
        //                        }
        //                    });
        //                }
        //                execs.add(collectProfileInfo);
        //            }
        //        }

        //collect cc and nc logs
        execs.add(new LogAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME, localExperimentRoot
                .resolve(LSMExperimentConstants.LOG_DIR + "-" + logDirSuffix).resolve(getName()).toString()));

        //get query result file
        final String queryResultFilePath = outputFilePath;
        execs.add(new AbstractRemoteExecutableAction(restHost, username, sshKeyLocation) {
            @Override
            protected String getCommand() {
                String cmd = "mv "
                        + queryResultFilePath
                        + " "
                        + localExperimentRoot.resolve(LSMExperimentConstants.LOG_DIR + "-" + logDirSuffix)
                                .resolve(getName()).toString();
                return cmd;
            }
        });
        //close the outputStream
        execs.add(new CloseOutputStreamAction(outputFos));

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

    private SequentialActionList getSelectQuery(boolean isIndexOnlyPlan) throws IOException {
        //prepare radius and center point
        int skipLineCount = SKIP_LINE_COUNT;
        int lineCount = 0;
        String line = null;;

        querySeed += SKIP_LINE_COUNT;
        if (querySeed > MAX_QUERY_SEED) {
            querySeed -= MAX_QUERY_SEED;
        }

        while (lineCount < skipLineCount) {
            if ((line = br.readLine()) == null) {
                //reopen file
                br.close();
                br = new BufferedReader(new FileReader(querySeedFilePath));
                line = br.readLine();
            }
            lineCount++;
        }

        int beginIdx = line.indexOf("(", line.indexOf("point"));
        int endIdx = line.indexOf(")", line.indexOf("point")) + 1;
        String point = line.substring(beginIdx, endIdx);

        //create action
        SequentialActionList sAction = new SequentialActionList();
        IAction queryAction = new TimedAction(new RunAQLStringAction(httpClient, restHost, restPort, getSelectQueryAQL(
                radiusType[radiusIter++ % radiusType.length], point, isIndexOnlyPlan), outputFos), outputFos);
        sAction.add(queryAction);

        return sAction;
    }

    private String getSelectQueryAQL(float radius, String point, boolean isIndexOnlyPlan) {
        if (PROFILE_JOB_LAUCHING_OVERHEAD) {
            Random random = new Random();
            int btreeExtraFieldKey = random.nextInt();
            int rangeSize = (int) (radius * 100000000L);
            if (btreeExtraFieldKey == Integer.MIN_VALUE) {
                btreeExtraFieldKey = Integer.MIN_VALUE + 1;
            }
            if (btreeExtraFieldKey + rangeSize >= Integer.MAX_VALUE) {
                btreeExtraFieldKey = Integer.MAX_VALUE - rangeSize - 1;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("use dataverse experiments; ");
            sb.append("count( ");
            sb.append("for $x in dataset Tweets").append(" ");
            sb.append("where $x.btree-extra-field1 > int32(\"" + btreeExtraFieldKey
                    + "\") and $x.btree-extra-field1 < int32(\"" + (btreeExtraFieldKey + rangeSize) + "\")");
            sb.append("return $x ");
            sb.append(");");

            System.out.println("[squery" + (queryCount++) + "]" + sb.toString());

            return sb.toString();
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("use dataverse experiments; ");
            sb.append("count( ");
            sb.append("for $x in dataset Tweets").append(" ");
            sb.append("let $n :=  create-circle( ");
            sb.append("point").append(point).append(" ");
            sb.append(", ");
            sb.append(String.format("%f", radius));
            sb.append(" ) ");
            if (isIndexOnlyPlan) {
                sb.append("where spatial-intersect($x.sender-location, $n) ");
            } else {
                sb.append("where spatial-intersect($x.sender-location, $n) and $x.btree-extra-field1 <= int32(\"2147483647\") ");
            }
            sb.append("return $x ");
            sb.append(");");

            System.out.println("[squery" + (queryCount++) + "]" + sb.toString());

            return sb.toString();
        }
    }

    private SequentialActionList getJoinQuery(boolean isIndexOnlyPlan) {
        querySeed += SKIP_LINE_COUNT;
        if (querySeed > MAX_QUERY_SEED) {
            querySeed -= MAX_QUERY_SEED;
        }

        int lowId = querySeed * 10000 + 1;
        int highId = (querySeed + JOIN_CANDIDATE_COUNT) * 10000 + 1;

        //create action
        SequentialActionList sAction = new SequentialActionList();
        IAction queryAction = new TimedAction(new RunAQLStringAction(httpClient, restHost, restPort, getJoinQueryAQL(
                radiusType[radiusIter++ % (radiusType.length - 1)], lowId, highId, isIndexOnlyPlan), outputFos),
                outputFos);
        sAction.add(queryAction);

        return sAction;
    }

    private String getJoinQueryAQL(float radius, int lowId, int highId, boolean isIndexOnlyPlan) {
        if (PROFILE_JOB_LAUCHING_OVERHEAD) {
            Random random = new Random();
            int btreeExtraFieldKey = random.nextInt();
            if (btreeExtraFieldKey == Integer.MIN_VALUE) {
                btreeExtraFieldKey = Integer.MIN_VALUE + 1;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("use dataverse experiments; ");
            sb.append("count( ");
            sb.append("for $x in dataset Tweets").append(" ");
            sb.append("where $x.tweetid = int64(\"" + btreeExtraFieldKey + "\")");
            sb.append("return $x ");
            sb.append(");");

            System.out.println("[squery" + (queryCount++) + "]" + sb.toString());

            return sb.toString();
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(" use dataverse experiments; \n");
            sb.append(" count( \n");
            sb.append(" for $x in dataset JoinSeedTweets").append(" \n");
            sb.append(" let $area := create-circle($x.sender-location, ").append(String.format("%f", radius))
                    .append(" ) \n");
            sb.append(" for $y in dataset Tweets \n");
            sb.append(" where $x.tweetid >= int64(\"" + lowId + "\") ").append(
                    "and $x.tweetid < int64(\"" + highId + "\") and ");
            if (isIndexOnlyPlan) {
                sb.append(" spatial-intersect($y.sender-location, $area) \n");
            } else {
                sb.append(" spatial-intersect($y.sender-location, $area) and $y.btree-extra-field1 <= int32(\"2147483647\")  \n");
            }
            sb.append(" return $y \n");
            sb.append(" );\n");

            System.out.println("[jquery" + (queryCount++) + "]" + sb.toString());

            return sb.toString();
        }
    }
}
