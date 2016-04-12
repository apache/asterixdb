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

package org.apache.asterix.metadata.entities;

import java.io.Serializable;

/**
 * Metadata describing a compute node.
 */
public class Node implements Serializable {

    private static final long serialVersionUID = 1L;
    // Enforced to be unique within a Hyracks cluster.
    private final String nodeName;
    private final long numberOfCores;
    private final long workingMemorySize;

    public Node(String nodeName, long noOfCores, long workingMemorySize) {
        this.nodeName = nodeName;
        this.numberOfCores = noOfCores;
        this.workingMemorySize = workingMemorySize;
    }

    public long getWorkingMemorySize() {
        return workingMemorySize;
    }

    public long getNumberOfCores() {
        return numberOfCores;
    }

    public String getNodeName() {
        return nodeName;
    }
}
