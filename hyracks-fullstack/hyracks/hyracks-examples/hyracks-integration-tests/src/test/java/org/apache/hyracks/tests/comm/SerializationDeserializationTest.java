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
package org.apache.hyracks.tests.comm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IDataWriter;
import org.apache.hyracks.api.dataflow.IOpenableDataReader;
import org.apache.hyracks.api.dataflow.IOpenableDataWriter;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.dataflow.common.comm.io.FrameDeserializingDataReader;
import org.apache.hyracks.dataflow.common.comm.io.SerializingDataWriter;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class SerializationDeserializationTest {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final String DBLP_FILE =
            "data" + File.separator + "device1" + File.separator + "data" + File.separator + "dblp.txt";

    private static class SerDeserRunner {
        private final IHyracksTaskContext ctx;
        private static final int FRAME_SIZE = 32768;
        private RecordDescriptor rDes;
        private List<IFrame> buffers;

        public SerDeserRunner(RecordDescriptor rDes) throws HyracksException {
            ctx = TestUtils.create(FRAME_SIZE);
            this.rDes = rDes;
            buffers = new ArrayList<>();
        }

        public IOpenableDataWriter<Object[]> createWriter() throws HyracksDataException {
            return new SerializingDataWriter(ctx, rDes, new IFrameWriter() {
                @Override
                public void open() throws HyracksDataException {
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    IFrame toBuf = new VSizeFrame(ctx);
                    toBuf.getBuffer().put(buffer);
                    buffers.add(toBuf);
                }

                @Override
                public void close() throws HyracksDataException {
                }

                @Override
                public void fail() throws HyracksDataException {
                }

                @Override
                public void flush() throws HyracksDataException {
                }
            });
        }

        public IOpenableDataReader<Object[]> createDataReader() throws HyracksDataException {
            return new FrameDeserializingDataReader(ctx, new IFrameReader() {
                private int i;

                @Override
                public void open() throws HyracksDataException {
                    i = 0;
                }

                @Override
                public boolean nextFrame(IFrame frame) throws HyracksDataException {
                    if (i < buffers.size()) {
                        IFrame buf = buffers.get(i);
                        buf.getBuffer().flip();
                        frame.getBuffer().put(buf.getBuffer());
                        frame.getBuffer().flip();
                        ++i;
                        return true;
                    }
                    return false;
                }

                @Override
                public void close() throws HyracksDataException {

                }
            }, rDes);
        }
    }

    private interface LineProcessor {
        public void process(String line, IDataWriter<Object[]> writer) throws Exception;
    }

    private void run(RecordDescriptor rDes, LineProcessor lp) throws Exception {
        SerDeserRunner runner = new SerDeserRunner(rDes);
        IOpenableDataWriter<Object[]> writer = runner.createWriter();
        writer.open();
        BufferedReader in = new BufferedReader(new FileReader(DBLP_FILE));
        String line;
        while ((line = in.readLine()) != null) {
            lp.process(line, writer);
        }
        writer.close();

        IOpenableDataReader<Object[]> reader = runner.createDataReader();
        reader.open();
        Object[] arr;
        while ((arr = reader.readData()) != null) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(arr[0] + " " + arr[1]);
            }
        }
        reader.close();
    }

    @Test
    public void serdeser01() throws Exception {
        RecordDescriptor rDes = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE });
        LineProcessor processor = new LineProcessor() {
            @Override
            public void process(String line, IDataWriter<Object[]> writer) throws Exception {
                String[] splits = line.split(" ");
                for (String s : splits) {
                    writer.writeData(new Object[] { s, Integer.valueOf(1) });
                }
            }
        };
        run(rDes, processor);
    }
}
