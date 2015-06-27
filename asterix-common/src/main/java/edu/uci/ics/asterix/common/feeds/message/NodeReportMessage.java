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
package edu.uci.ics.asterix.common.feeds.message;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.feeds.FeedConstants;
import edu.uci.ics.asterix.common.feeds.api.IFeedMessage;

public class NodeReportMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private double cpuLoad;
    private double usedHeap;
    private int nRuntimes;

    public NodeReportMessage(float cpuLoad, long usedHeap, int nRuntimes) {
        super(IFeedMessage.MessageType.NODE_REPORT);
        this.usedHeap = usedHeap;
        this.cpuLoad = cpuLoad;
        this.nRuntimes = nRuntimes;
    }

    public void reset(double cpuLoad, double usedHeap, int nRuntimes) {
        this.cpuLoad = cpuLoad;
        this.usedHeap = usedHeap;
        this.nRuntimes = nRuntimes;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.CPU_LOAD, cpuLoad);
        obj.put(FeedConstants.MessageConstants.HEAP_USAGE, usedHeap);
        obj.put(FeedConstants.MessageConstants.N_RUNTIMES, nRuntimes);
        return obj;
    }

    public double getCpuLoad() {
        return cpuLoad;
    }

    public double getUsedHeap() {
        return usedHeap;
    }

    public int getnRuntimes() {
        return nRuntimes;
    }

}
