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
package edu.uci.ics.hyracks.examples.text;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class WordTupleParserFactory implements ITupleParserFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public ITupleParser createTupleParser(final IHyracksTaskContext ctx) {
        return new ITupleParser() {
            @Override
            public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
                try {
                    ByteBuffer frame = ctx.allocateFrame();
                    FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                    appender.reset(frame, true);
                    ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
                    DataOutput dos = tb.getDataOutput();

                    IValueParser utf8StringParser = UTF8StringParserFactory.INSTANCE.createValueParser();
                    WordCursor cursor = new WordCursor(new InputStreamReader(in));
                    while (cursor.nextWord()) {
                        tb.reset();
                        utf8StringParser.parse(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart, dos);
                        tb.addFieldEndOffset();
                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            FrameUtils.flushFrame(frame, writer);
                            appender.reset(frame, true);
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                throw new HyracksDataException("Record size (" + tb.getSize() + ") larger than frame size (" + appender.getBuffer().capacity() + ")");
                            }
                        }
                    }
                    if (appender.getTupleCount() > 0) {
                        FrameUtils.flushFrame(frame, writer);
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(e);
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
