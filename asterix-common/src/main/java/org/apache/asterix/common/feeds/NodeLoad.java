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
package org.apache.asterix.common.feeds;

import org.apache.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;

public class NodeLoad implements Comparable<NodeLoad> {

    private final String nodeId;

    private int nRuntimes;

    public NodeLoad(String nodeId) {
        this.nodeId = nodeId;
        this.nRuntimes = 0;
    }

    public void addLoad() {
        nRuntimes++;
    }

    public void removeLoad(FeedRuntimeType runtimeType) {
        nRuntimes--;
    }

    @Override
    public int compareTo(NodeLoad o) {
        if (this == o) {
            return 0;
        }
        return nRuntimes - o.getnRuntimes();
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getnRuntimes() {
        return nRuntimes;
    }

    public void setnRuntimes(int nRuntimes) {
        this.nRuntimes = nRuntimes;
    }

}
