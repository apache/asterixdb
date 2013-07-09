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
package edu.uci.ics.asterix.transaction.management.service.recovery;

import java.io.Serializable;

public class CheckpointObject implements Serializable, Comparable<CheckpointObject> {

    private static final long serialVersionUID = 1L;
 
    private final long checkpointLSN;
    private final long minMCTFirstLSN;
    private final int maxJobId;
    private final long timeStamp;

    public CheckpointObject(long checkpointLSN, long minMCTFirstLSN, int maxJobId, long timeStamp) {
        this.checkpointLSN = checkpointLSN;
        this.minMCTFirstLSN = minMCTFirstLSN;
        this.maxJobId = maxJobId;
        this.timeStamp = timeStamp;
    }
    
    public long getCheckpointLSN() {
        return checkpointLSN;
    }

    public long getMinMCTFirstLSN() {
        return minMCTFirstLSN;
    }

    public int getMaxJobId() {
        return maxJobId;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    @Override
    public int compareTo(CheckpointObject checkpointObject) {
        long compareTimeStamp = checkpointObject.getTimeStamp(); 
        
        //decending order
        long diff = compareTimeStamp - this.timeStamp;
        if (diff > 0) {
            return 1;
        } else if (diff == 0){
            return 0;
        } else {
            return -1;
        }
 
        //ascending order
        //return this.timeStamp - compareTimeStamp;
    }
}
