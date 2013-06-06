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
package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

public class CodepointIterator {
    public void reset(byte [] buf, int startPos) {
        this.buf = buf;
        this.curPos = startPos + 2;
        this.startPos = startPos;
        len = UTF8StringPointable.getUTFLength(buf, startPos);        
    }
    
    public int size() { return len; }

    private byte[] buf;
    private int curPos = 0;
    private int len = 0;
    private int startPos = 0;

    public int getCodePoint() {
        return UTF8ToCodePoint(buf, curPos);
    }

    public static int UTF8ToCodePoint(byte[] b, int s) {
        if (b[s] >> 7 == 0) {
            // 1 byte
            return b[s];
        } else if ((b[s] & 0xe0) == 0xc0) { /*
             * 0xe0 = 0b1110000
             */
            // 2 bytes
            return ((int) (b[s] & 0x1f)) << 6
                    | /*
                     * 0x3f = 0b00111111
                     */ ((int) (b[s + 1] & 0x3f));
        } else if ((b[s] & 0xf0) == 0xe0) {
            // 3bytes
            return ((int) (b[s] & 0xf)) << 12
                    | ((int) (b[s + 1] & 0x3f)) << 6
                    | ((int) (b[s + 2] & 0x3f));
        } else if ((b[s] & 0xf8) == 0xf0) {
            // 4bytes
            return ((int) (b[s] & 0x7)) << 18
                    | ((int) (b[s + 1] & 0x3f)) << 12
                    | ((int) (b[s + 2] & 0x3f)) << 6
                    | ((int) (b[s + 3] & 0x3f));
        } else if ((b[s] & 0xfc) == 0xf8) {
            // 5bytes
            return ((int) (b[s] & 0x3)) << 24
                    | ((int) (b[s + 1] & 0x3f)) << 18
                    | ((int) (b[s + 2] & 0x3f)) << 12
                    | ((int) (b[s + 3] & 0x3f)) << 6
                    | ((int) (b[s + 4] & 0x3f));
        } else if ((b[s] & 0xfe) == 0xfc) {
            // 6bytes
            return ((int) (b[s] & 0x1)) << 30
                    | ((int) (b[s + 1] & 0x3f)) << 24
                    | ((int) (b[s + 2] & 0x3f)) << 18
                    | ((int) (b[s + 3] & 0x3f)) << 12
                    | ((int) (b[s + 4] & 0x3f)) << 6
                    | ((int) (b[s + 5] & 0x3f));
        }
        return 0;
    }

    public void next() {
        int step = UTF8StringPointable.charSize(buf, curPos);
        if(step + curPos < len + 2 + startPos)
            curPos += step;
    }

    public boolean hasNext() {
        int step = UTF8StringPointable.charSize(buf, curPos);
        if(step + curPos < len + 2 + startPos)
            return true;   
        return false;
    }
    
    public static int compare(CodepointIterator ls, CodepointIterator rs) {
        CodepointIterator shortString = ls.size() < rs.size() ? ls : rs;
        
        while (true) {
            int c1 = ls.getCodePoint();
            int c2 = rs.getCodePoint();
            if (c1 != c2) {
                return c1 - c2;
            }
            if(shortString.hasNext()) {
                ls.next();
                rs.next();
            } else {
                break;
            }
        }
        return ls.size() - rs.size();
    }
}
