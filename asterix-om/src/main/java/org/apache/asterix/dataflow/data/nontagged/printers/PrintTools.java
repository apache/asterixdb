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
package org.apache.asterix.dataflow.data.nontagged.printers;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public class PrintTools {
    public static enum CASE {
        LOWER_CASE,
        UPPER_CASE,
    }


    public static void writeUTF8StringAsCSV(byte[] b, int s, int l, OutputStream os) throws IOException {
        int stringLength = UTF8StringPointable.getUTFLength(b, s);
        int position = s + 2; // skip 2 bytes containing string size
        int maxPosition = position + stringLength;
        os.write('"');
        while (position < maxPosition) {
            char c = UTF8StringPointable.charAt(b, position);
            int sz = UTF8StringPointable.charSize(b, position);
            if (c == '"') {
                os.write('"');
            }
            os.write(c);
            position += sz;
        }
        os.write('"');
    }

    public static void writeUTF8StringAsJSON(byte[] b, int s, int l, OutputStream os) throws IOException {
        int stringLength = UTF8StringPointable.getUTFLength(b, s);
        int position = s + 2; // skip 2 bytes containing string size
        int maxPosition = position + stringLength;
        os.write('"');
        while (position < maxPosition) {
            char c = UTF8StringPointable.charAt(b, position);
            int sz = UTF8StringPointable.charSize(b, position);
            switch (c) {
                // escape
                case '\b':
                    os.write('\\');
                    os.write('b');
                    position += sz;
                    break;
                case '\f':
                    os.write('\\');
                    os.write('f');
                    position += sz;
                    break;
                case '\n':
                    os.write('\\');
                    os.write('n');
                    position += sz;
                    break;
                case '\r':
                    os.write('\\');
                    os.write('r');
                    position += sz;
                    break;
                case '\t':
                    os.write('\\');
                    os.write('t');
                    position += sz;
                    break;
                case '\\':
                case '"':
                    os.write('\\');
                default:
                    switch (sz) {
                        case 1:
                            if (c <= (byte) 0x1f || c == (byte) 0x7f) {
                                // this is to print out "control code points" (single byte UTF-8 representation,
                                // value up to 0x1f or 0x7f) in the 'uXXXX' format
                                writeUEscape(os, c);
                                ++position;
                                sz = 0; // no more processing
                            }
                            break;

                        case 2:
                            // 2-byte encodings of some code points in modified UTF-8 as described in
                            // DataInput.html#modified-utf-8
                            //
                            //         110xxxxx  10xxxxxx
                            // U+0000     00000    000000   C0 80
                            // U+0080     00010    000000   C2 80
                            // U+009F     00010    011111   C2 9F
                            switch (b[position]) {
                                case (byte) 0xc0:
                                    if (b[position + 1] == (byte) 0x80) {
                                        // special treatment for the U+0000 code point as described in
                                        // DataInput.html#modified-utf-8
                                        writeUEscape(os, c);
                                        position += 2;
                                        sz = 0; // no more processing
                                    }
                                    break;
                                case (byte) 0xc2:
                                    if (b[position + 1] <= (byte) 0x9f) {
                                        // special treatment for the U+0080 to U+009F code points
                                        writeUEscape(os, c);
                                        position += 2;
                                        sz = 0; // no more processing
                                    }
                                    break;
                            }
                            break;
                    }
                    while (sz > 0) {
                        os.write(b[position]);
                        ++position;
                        --sz;
                    }
                    break;
            }
        }
        os.write('\"');
    }

    private static void writeUEscape(OutputStream os, char c) throws IOException {
        os.write('\\');
        os.write('u');
        os.write('0');
        os.write('0');
        os.write(hex((c >>> 4) & 0x0f, CASE.LOWER_CASE));
        os.write(hex(c & 0x0f, CASE.LOWER_CASE));
    }

    static byte hex(int i, CASE c) {
        switch (c) {
            case LOWER_CASE:
                return (byte) (i < 10 ? i + '0' : i + ('a' - 10));
            case UPPER_CASE:
                return (byte) (i < 10 ? i + '0' : i + ('A' - 10));
        }
        return Byte.parseByte(null);
    }

}
