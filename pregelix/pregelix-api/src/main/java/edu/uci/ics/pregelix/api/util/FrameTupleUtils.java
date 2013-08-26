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

package edu.uci.ics.pregelix.api.util;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class FrameTupleUtils {

    public static void flushTuple(FrameTupleAppender appender, ArrayTupleBuilder tb, IFrameWriter writer)
            throws HyracksDataException {
        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            FrameUtils.flushFrame(appender.getBuffer(), writer);
            appender.reset(appender.getBuffer(), true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new IllegalStateException();
            }
        }
    }

    public static void flushTuplesFinal(FrameTupleAppender appender, IFrameWriter writer) throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(appender.getBuffer(), writer);
            appender.reset(appender.getBuffer(), true);
        }
    }

    public static void flushTupleToHDFS(ArrayTupleBuilder atb, Configuration conf, long superStep)
            throws HyracksDataException {
        try {
            if (atb.getSize()>0) {
                FileSystem dfs = FileSystem.get(conf);
                String fileName = BspUtils.getGlobalAggregateSpillingDirName(conf, superStep) +"/" + UUID.randomUUID();
                FSDataOutputStream dos = dfs.create(new Path(fileName), true);
                dos.write(atb.getByteArray(), 0, atb.getSize());
                dos.flush();
                dos.close();
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}
