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
