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

import java.util.ArrayList;
import java.util.List;

public class HyracksCluster {
    private String name;

    private ClusterController cc;

    private List<NodeController> ncs;

    public HyracksCluster() {
        ncs = new ArrayList<NodeController>();
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setClusterController(ClusterController cc) {
        this.cc = cc;
    }

    public ClusterController getClusterController() {
        return cc;
    }

    public void addNodeController(NodeController nc) {
        ncs.add(nc);
    }

    public List<NodeController> getNodeControllers() {
        return ncs;
    }
}