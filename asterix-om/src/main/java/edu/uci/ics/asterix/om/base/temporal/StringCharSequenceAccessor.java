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

public class StringCharSequenceAccessor implements ICharSequenceAccessor<String> {

    private String string;
    private int offset;
    private int length;

    @Override
    public char getCharAt(int index) throws AsterixRuntimeException {
        if (index >= length) {
            throw new AsterixRuntimeException("String accessor is out of bound.");
        }
        return string.charAt(index + offset);
    }

    public void reset(String obj, int offset, int len) {
        this.string = obj;
        this.offset = offset;
        this.length = len;
    }

    @Override
    public int getLength() {
        return length;
    }

}
