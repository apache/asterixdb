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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.experiment.action.base.SequentialActionList;
import org.apache.asterix.hyracks.bootstrap.CCApplicationEntryPoint;
import org.apache.asterix.hyracks.bootstrap.NCApplicationEntryPoint;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;

public abstract class AbstractLocalExperimentBuilder extends AbstractExperimentBuilder {

    private final int nNodeControllers;

    protected AbstractLocalExperimentBuilder(String name, int nNodeControllers) {
        super(name);
        this.nNodeControllers = nNodeControllers;
    }

    protected abstract void addPre(SequentialActionList pre);

    protected abstract void addPost(SequentialActionList post);

//    @Override
//    protected void prePost(SequentialExecutableSet pre, SequentialExecutableSet post) {
//        int ccClientPort = 1098;
//        int ccClusterPort = 1099;
//        CCConfig ccConfig = new CCConfig();
//        ccConfig.clusterNetIpAddress = "127.0.0.1";
//        ccConfig.clientNetIpAddress = "127.0.0.1";
//        ccConfig.clientNetPort = ccClientPort;
//        ccConfig.clusterNetPort = ccClusterPort;
//        ccConfig.defaultMaxJobAttempts = 0;
//        ccConfig.resultTTL = 30000;
//        ccConfig.resultSweepThreshold = 1000;
//        ccConfig.appCCMainClass = CCApplicationEntryPoint.class.getName();
//        final ClusterControllerService cc;
//        try {
//            cc = new ClusterControllerService(ccConfig);
//        } catch (Exception e) {
//            throw new IllegalArgumentException(e);
//        }
//
//        final List<NodeControllerService> ncs = new ArrayList<>();
//        for (int i = 0; i < nNodeControllers; ++i) {
//            NCConfig ncConfig = new NCConfig();
//            ncConfig.ccHost = "localhost";
//            ncConfig.ccPort = ccClusterPort;
//            ncConfig.clusterNetIPAddress = "127.0.0.1";
//            ncConfig.dataIPAddress = "127.0.0.1";
//            ncConfig.datasetIPAddress = "127.0.0.1";
//            ncConfig.nodeId = "nc" + String.valueOf((i + 1));
//            ncConfig.resultTTL = 30000;
//            ncConfig.resultSweepThreshold = 1000;
//            Path p0 = Paths.get(System.getProperty("java.io.tmpdir"), ncConfig.nodeId, "iodevice0");
//            Path p1 = Paths.get(System.getProperty("java.io.tmpdir"), ncConfig.nodeId, "iodevice1");
//            ncConfig.ioDevices = p0.toString() + "," + p1.toString();
//            ncConfig.appNCMainClass = NCApplicationEntryPoint.class.getName();
//            NodeControllerService nc;
//            try {
//                nc = new NodeControllerService(ncConfig);
//            } catch (Exception e) {
//                throw new IllegalArgumentException(e);
//            }
//            ncs.add(nc);
//        }
//
//        pre.add(new AbstractExecutable() {
//
//            @Override
//            protected void doExecute() throws Exception {
//                cc.start();
//                for (NodeControllerService nc : ncs) {
//                    nc.start();
//                }
//            }
//        });
//
//        post.add(new AbstractExecutable() {
//
//            @Override
//            protected void doExecute() throws Exception {
//                Collections.reverse(ncs);
//                for (NodeControllerService nc : ncs) {
//                    nc.stop();
//                }
//                cc.stop();
//                System.exit(1);
//            }
//        });
//        addPre(pre);
//        addPost(post);
//    }
}
