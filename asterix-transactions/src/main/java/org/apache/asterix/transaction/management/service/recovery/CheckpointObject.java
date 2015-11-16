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
package org.apache.asterix.transaction.management.service.recovery;

import java.io.Serializable;

public class CheckpointObject implements Serializable, Comparable<CheckpointObject> {

    private static final long serialVersionUID = 1L;

    private final long checkpointLsn;
    private final long minMCTFirstLsn;
    private final int maxJobId;
    private final long timeStamp;
    private final boolean sharp;
    
    public CheckpointObject(long checkpointLsn, long minMCTFirstLsn, int maxJobId, long timeStamp, boolean sharp) {
        this.checkpointLsn = checkpointLsn;
        this.minMCTFirstLsn = minMCTFirstLsn;
        this.maxJobId = maxJobId;
        this.timeStamp = timeStamp;
        this.sharp = sharp;
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

    public boolean isSharp() {
        return sharp;
    }
    
    @Override
    public int compareTo(CheckpointObject checkpointObject) {
        long compareTimeStamp = checkpointObject.getTimeStamp();

        //decending order
        long diff = compareTimeStamp - this.timeStamp;
        if (diff > 0) {
            return 1;
        } else if (diff == 0) {
            return 0;
        } else {
            return -1;
        }

        //ascending order
        //return this.timeStamp - compareTimeStamp;
    }
}
