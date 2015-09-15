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
package org.apache.asterix.common.feeds;

import java.util.Stack;

import org.apache.asterix.common.feeds.api.IFeedMemoryComponent;
import org.apache.asterix.common.feeds.api.IFeedMemoryManager;

/**
 * Represents a pool of reusable {@link DataBucket}
 */
public class DataBucketPool implements IFeedMemoryComponent {

    /** A unique identifier for the memory component **/
    private final int componentId;

    /** The {@link IFeedMemoryManager} for the NodeController **/
    private final IFeedMemoryManager memoryManager;

    /** A collection of available data buckets {@link DataBucket} **/
    private final Stack<DataBucket> pool;

    /** The total number of data buckets {@link DataBucket} allocated **/
    private int totalAllocation;

    /** The fixed frame size as configured for the asterix runtime **/
    private final int frameSize;

    public DataBucketPool(int componentId, IFeedMemoryManager memoryManager, int size, int frameSize) {
        this.componentId = componentId;
        this.memoryManager = memoryManager;
        this.pool = new Stack<DataBucket>();
        this.frameSize = frameSize;
        expand(size);
    }

    public synchronized void returnDataBucket(DataBucket bucket) {
        pool.push(bucket);
    }

    public synchronized DataBucket getDataBucket() {
        if (pool.size() == 0) {
            if (!memoryManager.expandMemoryComponent(this)) {
                return null;
            }
        }
        return pool.pop();
    }

    @Override
    public Type getType() {
        return Type.POOL;
    }

    @Override
    public int getTotalAllocation() {
        return totalAllocation;
    }

    @Override
    public int getComponentId() {
        return componentId;
    }

    @Override
    public void expand(int delta) {
        for (int i = 0; i < delta; i++) {
            DataBucket bucket = new DataBucket(this);
            pool.add(bucket);
        }
        totalAllocation += delta;
    }

    @Override
    public void reset() {
        totalAllocation -= pool.size();
        pool.clear();
    }

    @Override
    public String toString() {
        return "DataBucketPool" + "[" + componentId + "]" + "(" + totalAllocation + ")";
    }

    public int getSize() {
        return pool.size();
    }

    public int getFrameSize() {
        return frameSize;
    }

}