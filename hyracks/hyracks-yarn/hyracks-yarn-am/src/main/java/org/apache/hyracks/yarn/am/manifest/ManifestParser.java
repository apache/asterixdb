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
package org.apache.hyracks.yarn.am.manifest;

import java.io.StringReader;

import org.apache.commons.digester.Digester;

public class ManifestParser {
    public static HyracksCluster parse(String mXML) throws Exception {
        Digester d = createDigester();
        return (HyracksCluster) d.parse(new StringReader(mXML));
    }

    private static Digester createDigester() {
        Digester d = new Digester();
        d.setValidating(false);

        d.addObjectCreate("hyracks-cluster", HyracksCluster.class);
        d.addSetProperties("hyracks-cluster");

        d.addObjectCreate("hyracks-cluster/cluster-controller", ClusterController.class);
        d.addSetProperties("hyracks-cluster/cluster-controller");
        d.addSetNext("hyracks-cluster/cluster-controller", "setClusterController");

        d.addObjectCreate("hyracks-cluster/node-controllers/node-controller", NodeController.class);
        d.addSetProperties("hyracks-cluster/node-controllers/node-controller");
        d.addSetNext("hyracks-cluster/node-controllers/node-controller", "addNodeController");

        d.addObjectCreate("*/container-specification", ContainerSpecification.class);
        d.addSetProperties("*/container-specification");
        d.addSetNext("*/container-specification", "setContainerSpecification");
        return d;
    }
}