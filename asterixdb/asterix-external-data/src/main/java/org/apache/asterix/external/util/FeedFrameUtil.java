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
package org.apache.asterix.external.util;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Random;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class FeedFrameUtil {
    public static ByteBuffer removeBadTuple(IHyracksTaskContext ctx, int tupleIndex, FrameTupleAccessor fta)
            throws HyracksDataException {
        FrameTupleAppender appender = new FrameTupleAppender();
        IFrame slicedFrame = new VSizeFrame(ctx);
        appender.reset(slicedFrame, true);
        int totalTuples = fta.getTupleCount();
        for (int ti = 0; ti < totalTuples; ti++) {
            if (ti != tupleIndex) {
                appender.append(fta, ti);
            }
        }
        return slicedFrame.getBuffer();
    }

    public static ByteBuffer getSampledFrame(IHyracksTaskContext ctx, FrameTupleAccessor fta, int sampleSize)
            throws HyracksDataException {
        NChooseKIterator it = new NChooseKIterator(fta.getTupleCount(), sampleSize);
        FrameTupleAppender appender = new FrameTupleAppender();
        IFrame sampledFrame = new VSizeFrame(ctx);
        appender.reset(sampledFrame, true);
        int nextTupleIndex = 0;
        while (it.hasNext()) {
            nextTupleIndex = it.next();
            appender.append(fta, nextTupleIndex);
        }
        return sampledFrame.getBuffer();
    }

    private static class NChooseKIterator {

        private final int n;
        private final int k;
        private final BitSet bSet;
        private final Random random;

        private int traversed = 0;

        public NChooseKIterator(int n, int k) {
            this.n = n;
            this.k = k;
            this.bSet = new BitSet(n);
            bSet.set(0, n - 1);
            this.random = new Random();
        }

        public boolean hasNext() {
            return traversed < k;
        }

        public int next() {
            if (hasNext()) {
                traversed++;
                int startOffset = random.nextInt(n);
                int pos = -1;
                while (pos < 0) {
                    pos = bSet.nextSetBit(startOffset);
                    if (pos < 0) {
                        startOffset = 0;
                    }
                }
                bSet.clear(pos);
                return pos;
            } else {
                return -1;
            }
        }

    }

}
