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
package edu.uci.ics.hyracks.dataflow.std.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import edu.uci.ics.hyracks.api.comm.IFrame;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class DelimitedDataTupleParserFactory implements ITupleParserFactory {
    private static final long serialVersionUID = 1L;
    private IValueParserFactory[] valueParserFactories;
    private char fieldDelimiter;
    private char quote;

    public DelimitedDataTupleParserFactory(IValueParserFactory[] fieldParserFactories, char fieldDelimiter) {
        this(fieldParserFactories, fieldDelimiter, '\"');
    }

    public DelimitedDataTupleParserFactory(IValueParserFactory[] fieldParserFactories, char fieldDelimiter,
            char quote) {
        this.valueParserFactories = fieldParserFactories;
        this.fieldDelimiter = fieldDelimiter;
        this.quote = quote;
    }

    @Override
    public ITupleParser createTupleParser(final IHyracksTaskContext ctx) {
        return new ITupleParser() {
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
                            new InputStreamReader(in), fieldDelimiter, quote);
                    while (cursor.nextRecord()) {
                        tb.reset();
                        for (int i = 0; i < valueParsers.length; ++i) {
                            if (!cursor.nextField()) {
                                break;
                            }
                            // Eliminate double quotes in the field that we are going to parse
                            if (cursor.isDoubleQuoteIncludedInThisField) {
                                cursor.eliminateDoubleQuote(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart);
                                cursor.fEnd -= cursor.doubleQuoteCount;
                                cursor.isDoubleQuoteIncludedInThisField = false;
                            }
                            valueParsers[i].parse(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart, dos);
                            tb.addFieldEndOffset();
                        }
                        FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                                tb.getSize());
                    }
                    appender.flush(writer, true);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

        };
    }

}
