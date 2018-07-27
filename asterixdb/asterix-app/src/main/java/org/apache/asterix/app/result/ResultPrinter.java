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

import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.SessionConfig;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksAppendable;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class ResultPrinter {

    private final FrameManager resultDisplayFrameMgr;

    private final SessionOutput output;
    private final SessionConfig conf;
    private final Stats stats;
    private final ARecordType recordType;

    private boolean indentJSON;
    private boolean quoteRecord;

    // Whether we are wrapping the output sequence in an array
    private boolean wrapArray = false;
    // Whether this is the first instance being output
    private boolean notFirst = false;

    private ObjectMapper om;
    private ObjectWriter ow;

    public ResultPrinter(IApplicationContext appCtx, SessionOutput output, Stats stats, ARecordType recordType) {
        this.output = output;
        this.conf = output.config();
        this.stats = stats;
        this.recordType = recordType;
        this.indentJSON = conf.is(SessionConfig.FORMAT_INDENT_JSON);
        this.quoteRecord = conf.is(SessionConfig.FORMAT_QUOTE_RECORD);
        this.resultDisplayFrameMgr = new FrameManager(appCtx.getCompilerProperties().getFrameSize());
        if (indentJSON) {
            this.om = new ObjectMapper();
            DefaultPrettyPrinter.Indenter i = new DefaultPrettyPrinter.Indenter() {

                @Override
                public void writeIndentation(JsonGenerator jsonGenerator, int i) throws IOException {
                    jsonGenerator.writeRaw('\n');
                    for (int j = 0; j < i + 1; ++j) {
                        jsonGenerator.writeRaw('\t');
                    }
                }

                @Override
                public boolean isInline() {
                    return false;
                }
            };
            PrettyPrinter pp = new DefaultPrettyPrinter().withObjectIndenter(i).withArrayIndenter(i);
            this.ow = om.writer(pp);
        }
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
            throw HyracksDataException.create(e);
        }
    }

    private void printPrefix() throws HyracksDataException {
        // If we're outputting CSV with a header, the HTML header was already
        // output by displayCSVHeader(), so skip it here
        if (conf.is(SessionConfig.FORMAT_HTML)) {
            output.out().println("<h4>Results:</h4>");
            output.out().println("<pre class=\"result-content\">");
        }

        try {
            output.resultPrefix(new AlgebricksAppendable(output.out()));
        } catch (AlgebricksException e) {
            throw HyracksDataException.create(e);
        }

        if (conf.is(SessionConfig.FORMAT_WRAPPER_ARRAY)) {
            output.out().print("[ ");
            wrapArray = true;
        }

        if (conf.fmt() == SessionConfig.OutputFormat.CSV && conf.is(SessionConfig.FORMAT_CSV_HEADER)) {
            if (recordType == null) {
                throw new HyracksDataException("Cannot print CSV with header without specifying output-record-type");
            }
            if (quoteRecord) {
                StringWriter sw = new StringWriter();
                appendCSVHeader(sw, recordType);
                output.out().print(JSONUtil.quoteAndEscape(sw.toString()));
                output.out().print("\n");
                notFirst = true;
            } else {
                appendCSVHeader(output.out(), recordType);
            }
        }
    }

    private void printPostfix() throws HyracksDataException {
        output.out().flush();
        if (wrapArray) {
            output.out().println(" ]");
        }
        try {
            output.resultPostfix(new AlgebricksAppendable(output.out()));
        } catch (AlgebricksException e) {
            throw HyracksDataException.create(e);
        }
        if (conf.is(SessionConfig.FORMAT_HTML)) {
            output.out().println("</pre>");
        }
        output.out().flush();
    }

    private void displayRecord(String result) throws HyracksDataException {
        String record = result;
        if (indentJSON) {
            // TODO(tillw): this is inefficient - do this during record generation
            try {
                record = ow.writeValueAsString(om.readValue(result, Object.class));
            } catch (IOException e) { // NOSONAR if JSON parsing fails, just use the original string
                record = result;
            }
        }
        if (conf.fmt() == SessionConfig.OutputFormat.CSV) {
            // TODO(tillw): this is inefficient as well
            record = record + "\r\n";
        }
        if (quoteRecord) {
            // TODO(tillw): this is inefficient as well
            record = JSONUtil.quoteAndEscape(record);
        }
        if (conf.is(SessionConfig.FORMAT_HTML)) {
            record = ResultUtil.escapeHTML(record);
        }
        output.out().print(record);
        stats.setCount(stats.getCount() + 1);
        // TODO(tillw) fix this approximation
        stats.setSize(stats.getSize() + record.length());
    }

    public void print(String record) throws HyracksDataException {
        printPrefix();
        displayRecord(record);
        printPostfix();
    }

    public void print(ResultReader resultReader) throws HyracksDataException {
        printPrefix();
        try {
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
                        output.out().print(", ");
                    }
                    notFirst = true;
                    displayRecord(result);
                }
                frameBuffer.clear();
            }
        } finally {
            printPostfix();
        }
    }
}
