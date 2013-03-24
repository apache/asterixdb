package edu.uci.ics.hyracks.hdfs.api;

import org.apache.hadoop.mapred.InputSplit;

@SuppressWarnings("deprecation")
public interface INcCollection {

    public String findNearestAvailableSlot(InputSplit split);

    public int numAvailableSlots();
}
