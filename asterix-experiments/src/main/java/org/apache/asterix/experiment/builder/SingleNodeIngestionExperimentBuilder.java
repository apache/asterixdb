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

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;

import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.experiment.action.derived.RunAQLFileAction;
import org.apache.asterix.experiment.client.SocketDataGeneratorExecutable;

public class SingleNodeIngestionExperimentBuilder extends AbstractLocalExperimentBuilder {

    private final String adapterHost;

    private final int adapterPort;

    private final HttpClient httpClient;

    private final String restHost;

    private final int restPort;

    private final Path aqlFilePath;

    public SingleNodeIngestionExperimentBuilder(String adapterHost, int adapterPort, String restHost, int restPort,
            String aqlFilePath) {
        super("Local Ingestion Experiment", 2);
        this.adapterHost = adapterHost;
        this.adapterPort = adapterPort;
        httpClient = new DefaultHttpClient();
        this.restHost = restHost;
        this.restPort = restPort;
        this.aqlFilePath = Paths.get(aqlFilePath);
    }

    @Override
    protected void addPre(SequentialActionList pre) {
        pre.add(new RunAQLFileAction(httpClient, restHost, restPort, aqlFilePath));
    }

    @Override
    protected void addPost(SequentialActionList post) {

    }

    @Override
    protected void doBuild(Experiment e) {
        e.addBody(new SocketDataGeneratorExecutable(adapterHost, adapterPort));
    }
}
