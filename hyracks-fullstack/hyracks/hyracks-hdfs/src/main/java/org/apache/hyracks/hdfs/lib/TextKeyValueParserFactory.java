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

package org.apache.hyracks.hdfs.lib;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.hdfs.api.IKeyValueParser;
import org.apache.hyracks.hdfs.api.IKeyValueParserFactory;

public class TextKeyValueParserFactory implements IKeyValueParserFactory<LongWritable, Text> {
    private static final long serialVersionUID = 1L;

    @Override
    public IKeyValueParser<LongWritable, Text> createKeyValueParser(final IHyracksTaskContext ctx)
            throws HyracksDataException {

        final ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        final FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));

        return new IKeyValueParser<LongWritable, Text>() {

            @Override
            public void open(IFrameWriter writer) {

            }

            @Override
            public void parse(LongWritable key, Text value, IFrameWriter writer, String fileString)
                    throws HyracksDataException {
                tb.reset();
                tb.addField(value.getBytes(), 0, value.getLength());
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize());
            }

            @Override
            public void close(IFrameWriter writer) throws HyracksDataException {
                appender.write(writer, false);
            }

        };
    }

}
