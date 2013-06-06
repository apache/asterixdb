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
package edu.uci.ics.hyracks.dataflow.common.data.util;

import java.io.DataOutput;
import java.io.IOException;

public class StringUtils {
    public static int writeCharAsModifiedUTF8(char c, DataOutput dos) throws IOException {
        if (c >= 0x0000 && c <= 0x007F) {
            dos.writeByte(c);
            return 1;
        } else if (c <= 0x07FF) {
            dos.writeByte((byte) (0xC0 | ((c >> 6) & 0x3F)));
            dos.writeByte((byte) (0x80 | (c & 0x3F)));
            return 2;
        } else {
            dos.writeByte((byte) (0xE0 | ((c >> 12) & 0x0F)));
            dos.writeByte((byte) (0x80 | ((c >> 6) & 0x3F)));
            dos.writeByte((byte) (0x80 | (c & 0x3F)));
            return 3;
        }
    }

    public static void writeUTF8Len(int len, DataOutput dos) throws IOException {
        dos.write((len >>> 8) & 0xFF);
        dos.write((len >>> 0) & 0xFF);
    }
}