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
package org.apache.hyracks.dataflow.std.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;

/**
 * Currently used by tests only. Reconsider the code when it will be used otherwise.
 */
public class DelimitedDataTupleParserFactory implements ITupleParserFactory {
    private static final long serialVersionUID = 1L;
    private IValueParserFactory[] valueParserFactories;
    private char fieldDelimiter;
    private char quote;
    private char escape;

    public DelimitedDataTupleParserFactory(IValueParserFactory[] fieldParserFactories, char fieldDelimiter) {
        this(fieldParserFactories, fieldDelimiter, '\"', '\"');
    }

    public DelimitedDataTupleParserFactory(IValueParserFactory[] fieldParserFactories, char fieldDelimiter, char quote,
            char escape) {
        this.valueParserFactories = fieldParserFactories;
        this.fieldDelimiter = fieldDelimiter;
        this.quote = quote;
        this.escape = escape;
    }

    @Override
    public ITupleParser createTupleParser(final IHyracksTaskContext ctx) {
        return new ITupleParser() {
            final IWarningCollector warningCollector = ctx.getWarningCollector();

            @Override
            public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
                try {
                    IValueParser[] valueParsers = new IValueParser[valueParserFactories.length];
                    for (int i = 0; i < valueParserFactories.length; ++i) {
                        valueParsers[i] = valueParserFactories[i].createValueParser();
                    }
                    IFrame frame = new VSizeFrame(ctx);
                    FrameTupleAppender appender = new FrameTupleAppender();
                    appender.reset(frame, true);
                    ArrayTupleBuilder tb = new ArrayTupleBuilder(valueParsers.length);
                    DataOutput dos = tb.getDataOutput();

                    FieldCursorForDelimitedDataParser cursor = new FieldCursorForDelimitedDataParser(
                            new InputStreamReader(in), fieldDelimiter, quote, escape, warningCollector, () -> "", true);
                    while (cursor.nextRecord()) {
                        tb.reset();
                        for (int i = 0; i < valueParsers.length; ++i) {
                            FieldCursorForDelimitedDataParser.Result result = cursor.nextField();
                            switch (result) {
                                case OK:
                                    break;
                                case END:
                                case ERROR:
                                    throw new HyracksDataException("Failed to parse record");
                                default:
                                    throw new IllegalStateException();
                            }
                            // Eliminate escaped quotes in the field that we are going to parse
                            if (cursor.fieldHasEscapedQuote()) {
                                cursor.eliminateEscapeChar();
                            }
                            if (!valueParsers[i].parse(cursor.getBuffer(), cursor.getFieldStart(),
                                    cursor.getFieldLength(), dos)) {
                                throw new HyracksDataException("Failed to parse field");
                            }
                            tb.addFieldEndOffset();
                        }
                        FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                                tb.getSize());
                    }
                    appender.write(writer, true);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            }

        };
    }

}
