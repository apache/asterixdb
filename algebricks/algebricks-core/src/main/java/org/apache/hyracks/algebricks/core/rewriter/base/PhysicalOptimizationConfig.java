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
package edu.uci.ics.hyracks.algebricks.core.rewriter.base;

import java.util.Properties;

public class PhysicalOptimizationConfig {
    private static final int MB = 1048576;
    
    private static final String FRAMESIZE = "FRAMESIZE";
    private static final String MAX_FRAMES_EXTERNAL_SORT = "MAX_FRAMES_EXTERNAL_SORT";
    private static final String MAX_FRAMES_EXTERNAL_GROUP_BY = "MAX_FRAMES_EXTERNAL_GROUP_BY";
    private static final String MAX_FRAMES_LEFT_INPUT_HYBRID_HASH = "MAX_FRAMES_LEFT_INPUT_HYBRID_HASH";
    private static final String MAX_FRAMES_HYBRID_HASH = "MAX_FRAMES_HYBRID_HASH";
    private static final String FUDGE_FACTOR = "FUDGE_FACTOR";
    private static final String MAX_RECORDS_PER_FRAME = "MAX_RECORDS_PER_FRAME";
    
    private static final String DEFAULT_HASH_GROUP_TABLE_SIZE = "DEFAULT_HASH_GROUP_TABLE_SIZE";
    private static final String DEFAULT_EXTERNAL_GROUP_TABLE_SIZE = "DEFAULT_EXTERNAL_GROUP_TABLE_SIZE";
    private static final String DEFAULT_IN_MEM_HASH_JOIN_TABLE_SIZE = "DEFAULT_IN_MEM_HASH_JOIN_TABLE_SIZE";

    private Properties properties = new Properties();

    public PhysicalOptimizationConfig() {
        int frameSize = 32768;
        setInt(FRAMESIZE, frameSize);
        setInt(MAX_FRAMES_EXTERNAL_SORT, (int) (((long) 32 * MB) / frameSize));
        setInt(MAX_FRAMES_EXTERNAL_GROUP_BY, (int) (((long) 32 * MB) / frameSize));

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
    
    public double getFudgeFactor() {
        return getDouble(FUDGE_FACTOR, 1.3);
    }

    public void setFudgeFactor(double fudgeFactor) {
        setDouble(FUDGE_FACTOR, fudgeFactor);
    }
    
    public int getMaxRecordsPerFrame() {
        return getInt(MAX_RECORDS_PER_FRAME, 512);
    }

    public void setMaxRecordsPerFrame(int maxRecords) {
        setInt(MAX_RECORDS_PER_FRAME, maxRecords);
    }

    public int getMaxFramesLeftInputHybridHash() {
        int frameSize = getFrameSize();
        return getInt(MAX_FRAMES_LEFT_INPUT_HYBRID_HASH, (int) (140L * 1024 * MB / frameSize));
    }

    public void setMaxFramesLeftInputHybridHash(int frameLimit) {
        setInt(MAX_FRAMES_LEFT_INPUT_HYBRID_HASH, frameLimit);
    }
    
    public int getMaxFramesHybridHash() {
        int frameSize = getFrameSize();
        return getInt(MAX_FRAMES_HYBRID_HASH, (int) (64L * MB / frameSize));
    }

    public void setMaxFramesHybridHash(int frameLimit) {
        setInt(MAX_FRAMES_HYBRID_HASH, frameLimit);
    }

    public int getMaxFramesExternalGroupBy() {
        int frameSize = getFrameSize();
        return getInt(MAX_FRAMES_EXTERNAL_GROUP_BY, (int) (((long) 256 * MB) / frameSize));
    }

    public void setMaxFramesExternalGroupBy(int frameLimit) {
        setInt(MAX_FRAMES_EXTERNAL_GROUP_BY, frameLimit);
    }
    
    public int getMaxFramesExternalSort() {
        int frameSize = getFrameSize();
        return getInt(MAX_FRAMES_EXTERNAL_SORT, (int) (((long) 32 * MB) / frameSize));
    }

    public void setMaxFramesExternalSort(int frameLimit) {
        setInt(MAX_FRAMES_EXTERNAL_SORT, frameLimit);
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
    
    private void setDouble(String property, double value) {
        properties.setProperty(property, Double.toString(value));
    }

    private double getDouble(String property, double defaultValue) {
        String value = properties.getProperty(property);
        if (value == null)
            return defaultValue;
        else
            return Double.parseDouble(value);
    }

}
