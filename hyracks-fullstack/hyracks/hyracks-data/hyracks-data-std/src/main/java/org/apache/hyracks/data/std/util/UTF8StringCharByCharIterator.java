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

package org.apache.hyracks.data.std.util;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * An iterator class for a String. This class iterates a char by char in the given String.
 */
public class UTF8StringCharByCharIterator implements ISequenceIterator {

    protected byte[] data;
    protected int pos = -1;
    protected int length = -1;
    protected int utfByteLength = -1;
    protected int metaLength = -1;
    protected int startOffset = -1;

    @Override
    public int compare(ISequenceIterator cmpIter) throws HyracksDataException {
        char thisChar = Character.toLowerCase(UTF8StringUtil.charAt(data, pos));
        char thatChar = Character.toLowerCase(UTF8StringUtil.charAt(cmpIter.getData(), cmpIter.getPos()));
        if (thisChar == thatChar) {
            return 0;
        }
        return -1;
    }

    @Override
    public boolean hasNext() {
        return pos < utfByteLength;
    }

    @Override
    public int size() {
        return length;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public int getPos() {
        return pos;
    }

    @Override
    public void next() throws HyracksDataException {
        pos += UTF8StringUtil.charSize(data, pos);
    }

    @Override
    public void reset() throws HyracksDataException {
        pos = startOffset + metaLength;
    }

    @Override
    public void reset(byte[] data, int startOff) throws HyracksDataException {
        this.data = data;
        this.startOffset = startOff;
        this.length = UTF8StringUtil.getStringLength(data, startOffset);
        this.utfByteLength = UTF8StringUtil.getUTFLength(data, startOffset);
        this.metaLength = UTF8StringUtil.getNumBytesToStoreLength(utfByteLength);
        reset();
    }

}
