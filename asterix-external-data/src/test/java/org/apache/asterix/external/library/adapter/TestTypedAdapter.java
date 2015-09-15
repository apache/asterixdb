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
package org.apache.asterix.external.library.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.asterix.common.feeds.api.IFeedAdapter;
import org.apache.asterix.common.feeds.api.IFeedAdapter.DataExchangeMode;
import org.apache.asterix.external.dataset.adapter.StreamBasedAdapter;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

public class TestTypedAdapter extends StreamBasedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    private final PipedOutputStream pos;

    private final PipedInputStream pis;

    private final Map<String, String> configuration;

    private DummyGenerator generator;

    public TestTypedAdapter(ITupleParserFactory parserFactory, ARecordType sourceDatatype, IHyracksTaskContext ctx,
            Map<String, String> configuration, int partition) throws IOException {
        super(parserFactory, sourceDatatype, ctx, partition);
        pos = new PipedOutputStream();
        pis = new PipedInputStream(pos);
        this.configuration = configuration;
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        return pis;
    }

    @Override
    public void start(int partition, IFrameWriter frameWriter) throws Exception {
        generator = new DummyGenerator(configuration, pos);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(generator);
        super.start(partition, frameWriter);
    }

    private static class DummyGenerator implements Runnable {

        private final int nOutputRecords;
        private final OutputStream os;
        private final byte[] EOL = "\n".getBytes();
        private boolean continueIngestion;

        public DummyGenerator(Map<String, String> configuration, OutputStream os) {
            nOutputRecords = Integer.parseInt(configuration.get(TestTypedAdapterFactory.KEY_NUM_OUTPUT_RECORDS));
            this.os = os;
            this.continueIngestion = true;
        }

        @Override
        public void run() {
            DummyRecord dummyRecord = new DummyRecord();
            try {
                int i = 0;
                while (continueIngestion && i < nOutputRecords) {
                    dummyRecord.reset(i + 1, "" + (i + 1));
                    os.write(dummyRecord.toString().getBytes());
                    os.write(EOL);
                    i++;
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            } finally {
                try {
                    os.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public void stop() {
            continueIngestion = false;
        }
    }

    private static class DummyRecord {

        private int tweetid = 0;
        private String text = null;

        public void reset(int tweetid, String text) {
            this.tweetid = tweetid;
            this.text = text;
        }

        @Override
        public String toString() {
            return "{" + "\"tweetid\":" + "int64(" + "\"" + tweetid + "\"" + ")" + "," + "\"message-text\":" + "\""
                    + text + "\"" + "}";
        }

    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PUSH;
    }

    @Override
    public void stop() throws Exception {
        generator.stop();
    }

    @Override
    public boolean handleException(Exception e) {
        return false;
    }

}
