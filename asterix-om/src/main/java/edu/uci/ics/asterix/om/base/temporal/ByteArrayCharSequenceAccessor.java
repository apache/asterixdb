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

import edu.uci.ics.asterix.common.exceptions.AsterixRuntimeException;

public class ByteArrayCharSequenceAccessor implements ICharSequenceAccessor<Byte[]> {

    private byte[] buf;
    private int offset;
    private int length;

    @Override
    public char getCharAt(int index) throws AsterixRuntimeException {
        if (index < 0 || index >= length) {
            throw new AsterixRuntimeException("Byte array char accessor is out of bound: " + index + ":" + length);
        }
        return (char) (buf[index + offset]);
    }

    /**
     * Reset the wrapped byte array.
     * 
     * @param obj
     *            The byte array to be wrapped
     * @param beginOffset
     *            The offset of the string stored in the byte array.
     * @param offset
     *            The offset of the substring of the string stored (offset from the beginOffset).
     */
    public void reset(byte[] obj, int offset, int length) {
        this.buf = obj;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public int getLength() {
        return length;
    }

}
