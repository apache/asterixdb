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
package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class LockRequestTracker {
    HashMap<Integer, StringBuilder> historyPerJob; //per job
    StringBuilder historyForAllJobs;
    StringBuilder requestHistoryForAllJobs; //request only

    public LockRequestTracker() {
        historyForAllJobs = new StringBuilder();
        historyPerJob = new HashMap<Integer, StringBuilder>();
        requestHistoryForAllJobs = new StringBuilder();
    }

    public void addEvent(String msg, LockRequest request) {
        int jobId = request.txnContext.getJobId().getId();
        StringBuilder jobHistory = historyPerJob.get(jobId);

        //update jobHistory
        if (jobHistory == null) {
            jobHistory = new StringBuilder();
        }
        jobHistory.append(request.prettyPrint()).append("--> ").append(msg).append("\n");
        historyPerJob.put(jobId, jobHistory);

        //handle global request queue
        historyForAllJobs.append(request.prettyPrint()).append("--> ").append(msg).append("\n");
    }
    
    public void addRequest(LockRequest request) {
        requestHistoryForAllJobs.append(request.prettyPrint());
    }

    public String getHistoryForAllJobs() {
        return historyForAllJobs.toString();
    }

    public String getHistoryPerJob() {
        StringBuilder history = new StringBuilder();
        Set<Entry<Integer, StringBuilder>> s = historyPerJob.entrySet();
        Iterator<Entry<Integer, StringBuilder>> iter = s.iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer, StringBuilder> entry = (Map.Entry<Integer, StringBuilder>) iter.next();
            history.append(entry.getValue().toString());
        }
        return history.toString();
    }
    
    public String getRequestHistoryForAllJobs() {
        return requestHistoryForAllJobs.toString();
    }
}