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
package edu.uci.ics.asterix.installer.model;

import java.io.Serializable;

public class ProcessInfo implements Serializable {

    private static final long serialVersionUID = 304186774065853730L;
    private final String processName;
    private final String host;
    private final String nodeId;
    private final int processId;

    public ProcessInfo(String processName, String host, String nodeId, int processId) {
        this.processName = processName;
        this.host = host;
        this.nodeId = nodeId;
        this.processId = processId;
    }

    public String getProcessName() {
        return processName;
    }

    public String getHost() {
        return host;
    }

    public int getProcessId() {
        return processId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String toString() {
        return processName + " at " + nodeId + " [ " + processId + " ] ";
    }

}
