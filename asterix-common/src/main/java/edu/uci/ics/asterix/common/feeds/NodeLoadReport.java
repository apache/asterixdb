/*
 * Copyright 2009-2014 by The Regents of the University of California
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
package edu.uci.ics.asterix.common.feeds;

import org.json.JSONException;
import org.json.JSONObject;

public class NodeLoadReport implements Comparable<NodeLoadReport> {

    private final String nodeId;
    private float cpuLoad;
    private double usedHeap;
    private int nRuntimes;

    public NodeLoadReport(String nodeId, float cpuLoad, float usedHeap, int nRuntimes) {
        this.nodeId = nodeId;
        this.cpuLoad = cpuLoad;
        this.usedHeap = usedHeap;
        this.nRuntimes = nRuntimes;
    }

    public static NodeLoadReport read(JSONObject obj) throws JSONException {
        NodeLoadReport r = new NodeLoadReport(obj.getString(FeedConstants.MessageConstants.NODE_ID),
                (float) obj.getDouble(FeedConstants.MessageConstants.CPU_LOAD),
                (float) obj.getDouble(FeedConstants.MessageConstants.HEAP_USAGE),
                obj.getInt(FeedConstants.MessageConstants.N_RUNTIMES));
        return r;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NodeLoadReport)) {
            return false;
        }
        return ((NodeLoadReport) o).nodeId.equals(nodeId);
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }

    @Override
    public int compareTo(NodeLoadReport o) {
        if (nRuntimes != o.getnRuntimes()) {
            return nRuntimes - o.getnRuntimes();
        } else {
            return (int) (this.cpuLoad - ((NodeLoadReport) o).cpuLoad);
        }
    }

    public float getCpuLoad() {
        return cpuLoad;
    }

    public void setCpuLoad(float cpuLoad) {
        this.cpuLoad = cpuLoad;
    }

    public double getUsedHeap() {
        return usedHeap;
    }

    public void setUsedHeap(double usedHeap) {
        this.usedHeap = usedHeap;
    }

    public int getnRuntimes() {
        return nRuntimes;
    }

    public void setnRuntimes(int nRuntimes) {
        this.nRuntimes = nRuntimes;
    }

    public String getNodeId() {
        return nodeId;
    }

}
