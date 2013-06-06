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
package edu.uci.ics.hyracks.control.common.job.profiling.counters;

import java.io.Serializable;

public class MultiResolutionEventProfiler implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int[] times;

    private long offset;

    private int ptr;

    private int resolution;

    private int eventCounter;

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
}