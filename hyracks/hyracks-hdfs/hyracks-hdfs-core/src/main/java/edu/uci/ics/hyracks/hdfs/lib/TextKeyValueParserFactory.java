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

package edu.uci.ics.hyracks.hdfs.lib;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParser;
import edu.uci.ics.hyracks.hdfs.api.IKeyValueParserFactory;

public class TextKeyValueParserFactory implements IKeyValueParserFactory<LongWritable, Text> {
    private static final long serialVersionUID = 1L;

    @Override
    public IKeyValueParser<LongWritable, Text> createKeyValueParser(final IHyracksTaskContext ctx)
            throws HyracksDataException {

        final ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        final ByteBuffer buffer = ctx.allocateFrame();
        final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(buffer, true);

        return new IKeyValueParser<LongWritable, Text>() {

            @Override
            public void open(IFrameWriter writer) {

            }

            @Override
            public void parse(LongWritable key, Text value, IFrameWriter writer, String fileString)
                    throws HyracksDataException {
                tb.reset();
                tb.addField(value.getBytes(), 0, value.getLength());
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    FrameUtils.flushFrame(buffer, writer);
                    appender.reset(buffer, true);
                    if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                        throw new HyracksDataException("tuple cannot be appended into the frame");
                    }
                }
            }

            @Override
            public void close(IFrameWriter writer) throws HyracksDataException {
                FrameUtils.flushFrame(buffer, writer);
            }

        };
    }

}
