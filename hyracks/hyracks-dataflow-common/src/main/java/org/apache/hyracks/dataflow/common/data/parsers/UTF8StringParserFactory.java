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
package edu.uci.ics.hyracks.dataflow.common.data.parsers;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class UTF8StringParserFactory implements IValueParserFactory {
    public static final IValueParserFactory INSTANCE = new UTF8StringParserFactory();

    private static final long serialVersionUID = 1L;

    private UTF8StringParserFactory() {
    }

    @Override
    public IValueParser createValueParser() {
        return new IValueParser() {
            private byte[] utf8;

            @Override
            public void parse(char[] buffer, int start, int length, DataOutput out) throws HyracksDataException {
                int utflen = 0;
                for (int i = 0; i < length; i++) {
                    char ch = buffer[i + start];
                    if ((ch >= 0x0001) && (ch <= 0x007F)) {
                        utflen++;
                    } else if (ch > 0x07ff) {
                        utflen += 3;
                    } else {
                        utflen += 2;
                    }
                }

                if (utf8 == null || utf8.length < utflen + 2) {
                    utf8 = new byte[utflen + 2];
                }

                int count = 0;
                utf8[count++] = (byte) ((utflen >>> 8) & 0xff);
                utf8[count++] = (byte) ((utflen >>> 0) & 0xff);

                int i = 0;
                for (i = 0; i < length; i++) {
                    char ch = buffer[i + start];
                    if (!((ch >= 0x0001) && (ch <= 0x007F)))
                        break;
                    utf8[count++] = (byte) ch;
                }

                for (; i < length; i++) {
                    char ch = buffer[i + start];
                    if ((ch >= 0x0001) && (ch <= 0x007F)) {
                        utf8[count++] = (byte) ch;
                    } else if (ch > 0x07FF) {
                        utf8[count++] = (byte) (0xE0 | ((ch >> 12) & 0x0F));
                        utf8[count++] = (byte) (0x80 | ((ch >> 6) & 0x3F));
                        utf8[count++] = (byte) (0x80 | ((ch >> 0) & 0x3F));
                    } else {
                        utf8[count++] = (byte) (0xC0 | ((ch >> 6) & 0x1F));
                        utf8[count++] = (byte) (0x80 | ((ch >> 0) & 0x3F));
                    }
                }
                try {
                    out.write(utf8, 0, utflen + 2);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}