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
 
    private final long checkpointLsn;
    private final long minMCTFirstLsn;
    private final int maxJobId;
    private final long timeStamp;

    public CheckpointObject(long checkpointLsn, long minMCTFirstLsn, int maxJobId, long timeStamp) {
        this.checkpointLsn = checkpointLsn;
        this.minMCTFirstLsn = minMCTFirstLsn;
        this.maxJobId = maxJobId;
        this.timeStamp = timeStamp;
    }
    
    public long getCheckpointLsn() {
        return checkpointLsn;
    }

    public long getMinMCTFirstLsn() {
        return minMCTFirstLsn;
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
