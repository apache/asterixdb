package edu.uci.ics.hyracks.algebricks.core.rewriter.base;

import java.util.Properties;

public class PhysicalOptimizationConfig {
    private static final int MB = 1048576;
    private static final String FRAMESIZE = "FRAMESIZE";
    private static final String MAX_FRAMES_EXTERNAL_SORT = "MAX_FRAMES_EXTERNAL_SORT";
    private static final String MAX_FRAMES_EXTERNAL_GROUP_BY = "MAX_FRAMES_EXTERNAL_GROUP_BY";

    private static final String DEFAULT_HASH_GROUP_TABLE_SIZE = "DEFAULT_HASH_GROUP_TABLE_SIZE";
    private static final String DEFAULT_EXTERNAL_GROUP_TABLE_SIZE = "DEFAULT_EXTERNAL_GROUP_TABLE_SIZE";
    private static final String DEFAULT_IN_MEM_HASH_JOIN_TABLE_SIZE = "DEFAULT_IN_MEM_HASH_JOIN_TABLE_SIZE";

    private Properties properties = new Properties();

    public PhysicalOptimizationConfig() {
        int frameSize = 131072;
        setInt(FRAMESIZE, frameSize);
        setInt(MAX_FRAMES_EXTERNAL_SORT, (int) (((long) 340 * MB) / frameSize));
        setInt(MAX_FRAMES_EXTERNAL_GROUP_BY, (int) (((long) 340 * MB) / frameSize));

        // use http://www.rsok.com/~jrm/printprimes.html to find prime numbers
        setInt(DEFAULT_HASH_GROUP_TABLE_SIZE, 10485767);
        setInt(DEFAULT_EXTERNAL_GROUP_TABLE_SIZE, 10485767);
        setInt(DEFAULT_IN_MEM_HASH_JOIN_TABLE_SIZE, 10485767);
    }

    public int getFrameSize() {
        return getInt(FRAMESIZE, 32768);
    }

    public void setFrameSize(int frameSize) {
        setInt(FRAMESIZE, frameSize);
    }

    public int getMaxFramesExternalSort() {
        int frameSize = getFrameSize();
        return getInt(MAX_FRAMES_EXTERNAL_SORT, (int) (((long) 512 * MB) / frameSize));
    }

    public void setMaxFramesExternalSort(int frameLimit) {
        setInt(MAX_FRAMES_EXTERNAL_SORT, frameLimit);
    }

    public int getMaxFramesExternalGroupBy() {
        int frameSize = getFrameSize();
        return getInt(MAX_FRAMES_EXTERNAL_GROUP_BY, (int) (((long) 256 * MB) / frameSize));
    }

    public void setMaxFramesExternalGroupBy(int frameLimit) {
        setInt(MAX_FRAMES_EXTERNAL_GROUP_BY, frameLimit);
    }

    public int getHashGroupByTableSize() {
        return getInt(DEFAULT_HASH_GROUP_TABLE_SIZE, 10485767);
    }

    public void setHashGroupByTableSize(int tableSize) {
        setInt(DEFAULT_HASH_GROUP_TABLE_SIZE, tableSize);
    }

    public int getExternalGroupByTableSize() {
        return getInt(DEFAULT_EXTERNAL_GROUP_TABLE_SIZE, 10485767);
    }

    public void setExternalGroupByTableSize(int tableSize) {
        setInt(DEFAULT_EXTERNAL_GROUP_TABLE_SIZE, tableSize);
    }

    public int getInMemHashJoinTableSize() {
        return getInt(DEFAULT_IN_MEM_HASH_JOIN_TABLE_SIZE, 10485767);
    }

    public void setInMemHashJoinTableSize(int tableSize) {
        setInt(DEFAULT_IN_MEM_HASH_JOIN_TABLE_SIZE, tableSize);
    }

    private void setInt(String property, int value) {
        properties.setProperty(property, Integer.toString(value));
    }

    private int getInt(String property, int defaultValue) {
        String value = properties.getProperty(property);
        if (value == null)
            return defaultValue;
        else
            return Integer.parseInt(value);
    }

}
