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

import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.RunAQLFileAction;
import org.apache.asterix.experiment.action.derived.RunSQLPPFileAction;
import org.apache.asterix.experiment.action.derived.SleepAction;
import org.apache.asterix.experiment.action.derived.TimedAction;
import org.apache.asterix.experiment.client.LSMExperimentConstants;
import org.apache.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;
import org.apache.asterix.experiment.client.LSMPerfConstants;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * This experiment simply runs the performance benchmark queries against
 * a cluster which is presumed to already be running.
 * Also, there is no orchestration server involved in this experiment builder.
 *
 * The only LSMExperimentSetRunner config command-line options used by this
 * experiment are:
 *   -ler  -rh  -rp
 */
public class PresetClusterPerfBuilder extends AbstractExperimentBuilder {

    protected final HttpClient httpClient;

    protected final String restHost;

    protected final int restPort;

    protected final Path localExperimentRoot;

    private final String countFileName;

    protected final String loadAQLFilePath;

    protected final String querySQLPPFileName;

    public PresetClusterPerfBuilder(LSMExperimentSetRunnerConfig config) {
        super("PresetClusterPerfBuilder");
        this.httpClient = new DefaultHttpClient();
        this.restHost = config.getRESTHost();
        this.restPort = config.getRESTPort();
        this.localExperimentRoot = Paths.get(config.getLocalExperimentRoot());
        this.countFileName = "bench_count.aql";
        this.loadAQLFilePath = "bench_3_load_2.aql";
        this.querySQLPPFileName = "agg_bench";
    }

    @Override
    protected void doBuild(Experiment e) throws IOException, JAXBException {
        SequentialActionList execs = new SequentialActionList();

        //ddl statements
        execs.add(new SleepAction(15000));
        // TODO: implement retry handler
        execs.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve(LSMPerfConstants.BASE_TYPES)));
        execs.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve("bench_3.aql")));

        //---------- main experiment body begins -----------

        //run DDL + Load
        execs.add(new TimedAction(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve(loadAQLFilePath))));

        //execute SQL++ Queries
        execs.add(new TimedAction(new RunSQLPPFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve(querySQLPPFileName),
                localExperimentRoot.resolve(LSMPerfConstants.RESULT_FILE))));

        //---------- main experiment body ends -----------

        //total record count
        execs.add(new SleepAction(10000));
        if (countFileName != null) {
            execs.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                    LSMExperimentConstants.AQL_DIR).resolve(countFileName)));
        }

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
