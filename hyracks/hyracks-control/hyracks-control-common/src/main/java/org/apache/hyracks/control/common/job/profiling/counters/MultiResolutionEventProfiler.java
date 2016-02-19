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
package org.apache.hyracks.control.common.job.profiling.counters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hyracks.api.io.IWritable;

public class MultiResolutionEventProfiler implements IWritable, Serializable {
    private static final long serialVersionUID = 1L;

    private int[] times;

    private long offset;

    private int ptr;

    private int resolution;

    private int eventCounter;

    public static MultiResolutionEventProfiler create(DataInput dis) throws IOException {
        MultiResolutionEventProfiler multiResolutionEventProfiler = new MultiResolutionEventProfiler();
        multiResolutionEventProfiler.readFields(dis);
        return multiResolutionEventProfiler;
    }

    private MultiResolutionEventProfiler() {

    }

    public MultiResolutionEventProfiler(int nSamples) {
        times = new int[nSamples];
        offset = -1;
        ptr = 0;
        resolution = 1;
        eventCounter = 0;
    }

    public void reportEvent() {
        ++eventCounter;
        if (eventCounter % resolution != 0) {
            return;
        }
        if (ptr >= times.length) {
            compact();
            return;
        }
        eventCounter = 0;
        long time = System.currentTimeMillis();
        if (offset < 0) {
            offset = time;
        }
        int value = (int) (time - offset);
        times[ptr++] = value;
    }

    private void compact() {
        for (int i = 1; i < ptr / 2; ++i) {
            times[i] = times[i * 2];
        }
        resolution <<= 1;
        ptr >>= 1;
    }

    public int getResolution() {
        return resolution;
    }

    public int getCount() {
        return ptr;
    }

    public int[] getSamples() {
        return times;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeInt(eventCounter);
        output.writeLong(offset);
        output.writeInt(ptr);
        output.writeInt(resolution);
        output.writeInt(times.length);
        for (int i = 0; i < times.length; i++) {
            output.writeInt(times[i]);
        }
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        eventCounter = input.readInt();
        offset = input.readLong();
        ptr = input.readInt();
        resolution = input.readInt();
        int nSamples = input.readInt();
        times = new int[nSamples];
        for (int i = 0; i < times.length; i++) {
            times[i] = input.readInt();
        }
    }
}
