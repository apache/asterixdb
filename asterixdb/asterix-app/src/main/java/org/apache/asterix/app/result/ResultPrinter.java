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
package org.apache.asterix.app.result;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;

import org.apache.asterix.common.app.SessionConfig;
import org.apache.asterix.common.utils.JSONUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksAppendable;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;

public class ResultPrinter {

    // TODO(tillw): Should this be static?
    private static FrameManager resultDisplayFrameMgr = new FrameManager(ResultReader.FRAME_SIZE);

    private final SessionConfig conf;
    private final Stats stats;
    private final ARecordType recordType;

    private boolean indentJSON;
    private boolean quoteRecord;

    // Whether we are wrapping the output sequence in an array
    private boolean wrapArray = false;
    // Whether this is the first instance being output
    private boolean notFirst = false;

    public ResultPrinter(SessionConfig conf, Stats stats, ARecordType recordType) {
        this.conf = conf;
        this.stats = stats;
        this.recordType = recordType;
        this.indentJSON = conf.is(SessionConfig.FORMAT_INDENT_JSON);
        this.quoteRecord = conf.is(SessionConfig.FORMAT_QUOTE_RECORD);
    }

    private static void appendCSVHeader(Appendable app, ARecordType recordType) throws HyracksDataException {
        try {
            String[] fieldNames = recordType.getFieldNames();
            boolean notfirst = false;
            for (String name : fieldNames) {
                if (notfirst) {
                    app.append(',');
                }
                notfirst = true;
                app.append('"').append(name.replace("\"", "\"\"")).append('"');
            }
            app.append("\r\n");
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    private void printPrefix() throws HyracksDataException {
        // If we're outputting CSV with a header, the HTML header was already
        // output by displayCSVHeader(), so skip it here
        if (conf.is(SessionConfig.FORMAT_HTML)) {
            conf.out().println("<h4>Results:</h4>");
            conf.out().println("<pre>");
        }

        try {
            conf.resultPrefix(new AlgebricksAppendable(conf.out()));
        } catch (AlgebricksException e) {
            throw new HyracksDataException(e);
        }

        if (conf.is(SessionConfig.FORMAT_WRAPPER_ARRAY)) {
            conf.out().print("[ ");
            wrapArray = true;
        }

        if (conf.fmt() == SessionConfig.OutputFormat.CSV && conf.is(SessionConfig.FORMAT_CSV_HEADER)) {
            if (recordType == null) {
                throw new HyracksDataException("Cannot print CSV with header without specifying output-record-type");
            }
            if (quoteRecord) {
                StringWriter sw = new StringWriter();
                appendCSVHeader(sw, recordType);
                conf.out().print(JSONUtil.quoteAndEscape(sw.toString()));
                conf.out().print("\n");
                notFirst = true;
            } else {
                appendCSVHeader(conf.out(), recordType);
            }
        }
    }

    private void printPostfix() throws HyracksDataException {
        conf.out().flush();
        if (wrapArray) {
            conf.out().println(" ]");
        }
        try {
            conf.resultPostfix(new AlgebricksAppendable(conf.out()));
        } catch (AlgebricksException e) {
            throw new HyracksDataException(e);
        }
        if (conf.is(SessionConfig.FORMAT_HTML)) {
            conf.out().println("</pre>");
        }
    }

    private void displayRecord(String result) {
        String record = result;
        if (indentJSON) {
            // TODO(tillw): this is inefficient - do this during record generation
            record = JSONUtil.indent(record, 2);
        }
        if (conf.fmt() == SessionConfig.OutputFormat.CSV) {
            // TODO(tillw): this is inefficient as well
            record = record + "\r\n";
        }
        if (quoteRecord) {
            // TODO(tillw): this is inefficient as well
            record = JSONUtil.quoteAndEscape(record);
        }
        conf.out().print(record);
        stats.setCount(stats.getCount() + 1);
        // TODO(tillw) fix this approximation
        stats.setSize(stats.getSize() + record.length());
    }

    public void print(String record) throws HyracksDataException {
        printPrefix();
        // TODO(tillw) evil hack
        quoteRecord = true;
        displayRecord(record);
        printPostfix();
    }

    public void print(ResultReader resultReader) throws HyracksDataException {
        printPrefix();

        final IFrameTupleAccessor fta = resultReader.getFrameTupleAccessor();
        final IFrame frame = new VSizeFrame(resultDisplayFrameMgr);

        while (resultReader.read(frame) > 0) {
            final ByteBuffer frameBuffer = frame.getBuffer();
            final byte[] frameBytes = frameBuffer.array();
            fta.reset(frameBuffer);
            final int last = fta.getTupleCount();
            for (int tIndex = 0; tIndex < last; tIndex++) {
                final int start = fta.getTupleStartOffset(tIndex);
                int length = fta.getTupleEndOffset(tIndex) - start;
                if (conf.fmt() == SessionConfig.OutputFormat.CSV
                        && ((length > 0) && (frameBytes[start + length - 1] == '\n'))) {
                    length--;
                }
                String result = new String(frameBytes, start, length, UTF_8);
                if (wrapArray && notFirst) {
                    conf.out().print(", ");
                }
                notFirst = true;
                displayRecord(result);
            }
            frameBuffer.clear();
        }

        printPostfix();
    }
}
