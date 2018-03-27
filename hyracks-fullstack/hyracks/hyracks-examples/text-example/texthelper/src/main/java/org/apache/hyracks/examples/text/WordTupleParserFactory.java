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
package org.apache.hyracks.examples.text;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.std.file.ITupleParser;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

public class WordTupleParserFactory implements ITupleParserFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public ITupleParser createTupleParser(final IHyracksTaskContext ctx) {
        return new ITupleParser() {
            @Override
            public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
                try {
                    FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
                    ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
                    DataOutput dos = tb.getDataOutput();

                    IValueParser utf8StringParser = UTF8StringParserFactory.INSTANCE.createValueParser();
                    WordCursor cursor = new WordCursor(new InputStreamReader(in));
                    while (cursor.nextWord()) {
                        tb.reset();
                        utf8StringParser.parse(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart, dos);
                        tb.addFieldEndOffset();
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

    private static class WordCursor {
        private static final int INITIAL_BUFFER_SIZE = 4096;
        private static final int INCREMENT = 4096;

        private char[] buffer;

        private int start;
        private int end;
        private boolean eof;

        private int fStart;
        private int fEnd;
        private Reader in;

        public WordCursor(Reader in) {
            this.in = in;
            buffer = new char[INITIAL_BUFFER_SIZE];
            start = 0;
            end = 0;
            eof = false;
        }

        public boolean nextWord() throws IOException {
            if (eof) {
                return false;
            }

            boolean wordStarted = false;
            int p = start;
            while (true) {
                if (p >= end) {
                    int s = start;
                    eof = !readMore();
                    if (eof) {
                        return true;
                    }
                    p -= (s - start);
                }
                char ch = buffer[p];
                if (isNonWordChar(ch)) {
                    fStart = start;
                    fEnd = p;
                    start = p + 1;
                    if (wordStarted) {
                        return true;
                    }
                } else {
                    wordStarted = true;
                }
                ++p;
            }
        }

        private boolean isNonWordChar(char ch) {
            switch (ch) {
                case '.':
                case ',':
                case '!':
                case '@':
                case '#':
                case '$':
                case '%':
                case '^':
                case '&':
                case '*':
                case '(':
                case ')':
                case '+':
                case '=':
                case ':':
                case ';':
                case '"':
                case '\'':
                case '{':
                case '}':
                case '[':
                case ']':
                case '|':
                case '\\':
                case '/':
                case '<':
                case '>':
                case '?':
                case '~':
                case '`':
                    return true;
            }
            return Character.isWhitespace(ch);
        }

        private boolean readMore() throws IOException {
            if (start > 0) {
                System.arraycopy(buffer, start, buffer, 0, end - start);
            }
            end -= start;
            start = 0;

            if (end == buffer.length) {
                buffer = Arrays.copyOf(buffer, buffer.length + INCREMENT);
            }

            int n = in.read(buffer, end, buffer.length - end);
            if (n < 0) {
                return false;
            }
            end += n;
            return true;
        }
    }
}
