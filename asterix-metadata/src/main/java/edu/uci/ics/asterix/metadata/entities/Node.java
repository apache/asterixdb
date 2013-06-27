/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.metadata.entities;

import java.io.Serializable;

/**
 * Metadata describing a compute node.
 */
public class Node implements Serializable {

    private static final long serialVersionUID = 1L;
    // Enforced to be unique within a Hyracks cluster.
    private final String nodeName;
    private final int numberOfCores;
    private final int workingMemorySize;

    public Node(String nodeName, int noOfCores, int workingMemorySize) {
        this.nodeName = nodeName;
        this.numberOfCores = noOfCores;
        this.workingMemorySize = workingMemorySize;
    }

    public int getWorkingMemorySize() {
        return workingMemorySize;
    }

    public int getNumberOfCores() {
        return numberOfCores;
    }

    public String getNodeName() {
        return nodeName;
    }
}
