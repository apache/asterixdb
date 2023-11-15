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
package org.apache.hyracks.api.job.profiling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hyracks.api.job.profiling.counters.ICounter;

public class NoOpOperatorStats implements IOperatorStats {

    private static final long serialVersionUID = 9055940222300360256L;

    public static final NoOpOperatorStats INSTANCE = new NoOpOperatorStats();
    public static final String INVALID_ODID = "ODID:-1";
    public static final String NOOP_NAME = "NoOp";

    private static final ICounter NOOP_COUNTER = new ICounter() {
        private static final long serialVersionUID = 1L;

        @Override
        public long update(long delta) {
            return 0;
        }

        @Override
        public long set(long value) {
            return 0;
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public long get() {
            return 0;
        }
    };

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeUTF(NOOP_NAME);
        output.writeUTF(INVALID_ODID);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        // nothing
    }

    @Override
    public String getName() {
        return NOOP_NAME;
    }

    @Override
    public ICounter getTupleCounter() {
        return NOOP_COUNTER;
    }

    @Override
    public ICounter getTimeCounter() {
        return NOOP_COUNTER;
    }

    @Override
    public ICounter getPageReads() {
        return NOOP_COUNTER;
    }

    @Override
    public ICounter coldReadCounter() {
        return NOOP_COUNTER;
    }

    @Override
    public ICounter getAverageTupleSz() {
        return NOOP_COUNTER;
    }

    @Override
    public ICounter getMaxTupleSz() {
        return NOOP_COUNTER;
    }

    @Override
    public ICounter getMinTupleSz() {
        return NOOP_COUNTER;
    }

    @Override
    public ICounter getInputTupleCounter() {
        return NOOP_COUNTER;
    }

    @Override
    public ICounter getLevel() {
        return NOOP_COUNTER;
    }

    @Override
    public ICounter getBytesRead() {
        return NOOP_COUNTER;
    }

    @Override
    public ICounter getBytesWritten() {
        return NOOP_COUNTER;
    }

    @Override
    public String getOperatorId() {
        return INVALID_ODID;
    }

    @Override
    public void updateIndexesStats(Map<String, IndexStats> indexesStats) {
        // no op
    }

    @Override
    public Map<String, IndexStats> getIndexesStats() {
        return Collections.emptyMap();
    }

    @Override
    public void updateFrom(IOperatorStats stats) {
        // no op
    }
}
