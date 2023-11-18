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
package org.apache.hyracks.algebricks.runtime.base;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.dataflow.ITimedWriter;
import org.apache.hyracks.api.dataflow.ProfiledFrameWriter;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;

public class ProfiledPushRuntime extends ProfiledFrameWriter implements IPushRuntime {

    private final IPushRuntime wrapped;
    private final IOperatorStats stats;

    private final boolean last;

    private final Map<Integer, ITimedWriter> outputs;

    public ProfiledPushRuntime(IPushRuntime push, IOperatorStats stats, boolean last) {
        super(push);
        outputs = new HashMap<>();
        this.wrapped = push;
        this.stats = stats;
        this.last = last;
    }

    @Override
    public void close() throws HyracksDataException {
        super.close();
        long ownTime = getTotalTime();
        //for micro union all. accumulate the time of each input into the counter.
        //then, on input 0, subtract the output from the accumulated time.
        if (!last) {
            stats.getTimeCounter().update(ownTime);
            return;
        }
        ownTime += stats.getTimeCounter().get();
        for (ITimedWriter w : outputs.values()) {
            ownTime -= w.getTotalTime();
        }
        stats.getTimeCounter().set(ownTime);
    }

    public IOperatorStats getStats() {
        return stats;
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        if (writer instanceof ITimedWriter) {
            ITimedWriter wrapper = (ITimedWriter) writer;
            wrapper.setUpstreamStats(stats);
            outputs.put(index, wrapper);
        }
        wrapped.setOutputFrameWriter(index, writer, recordDesc);
    }

    @Override
    public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
        wrapped.setInputRecordDescriptor(index, recordDescriptor);
    }

    public static IPushRuntime time(IPushRuntime push, IOperatorStats stats, boolean last) throws HyracksDataException {
        if (!(push instanceof ProfiledPushRuntime)) {
            return new ProfiledPushRuntime(push, stats, last);
        } else {
            return push;
        }
    }

    public static IPushRuntime time(IPushRuntime push, IOperatorStats stats) throws HyracksDataException {
        return time(push, stats, true);
    }
}
