/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.om.base.temporal;

public class ByteArrayCharSequenceAccessor implements ICharSequenceAccessor<Byte[]> {

    private byte[] string;
    private int offset;
    private int beginOffset;

    @Override
    public char getCharAt(int index) {
        return (char) (string[index + offset + beginOffset]);
    }

    /* The offset is the position of the first letter in the byte array */
    public void reset(byte[] obj, int beginOffset, int offset) {
        string = obj;
        this.offset = offset;
        this.beginOffset = beginOffset;
    }

    @Override
    public int getLength() {
        return ((string[beginOffset - 2] & 0xff) << 8) + ((string[beginOffset - 1] & 0xff) << 0) - offset;
    }

}
