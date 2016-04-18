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

package org.apache.hyracks.data.std.primitive;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IComparable;
import org.apache.hyracks.data.std.api.IHashable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

public class ByteArrayPointable extends AbstractPointable implements IHashable, IComparable, Serializable {
    private static final long serialVersionUID = 1L;

    // These three values are cached to speed up the length data access.
    // Since the we are using the variable-length encoding, we can save the repeated decoding efforts.
    // WARNING: must call the resetConstants() method after each reset().
    private int contentLength = -1;
    private int metaLength = -1;
    private int hash = 0;

    @Override
    protected void afterReset() {
        contentLength = getContentLength(getByteArray(), getStartOffset());
        metaLength = getNumberBytesToStoreMeta(contentLength);
        hash = 0;
    }

    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return false;
        }

        @Override
        public int getFixedLength() {
            return 0;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new ByteArrayPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] thatBytes, int thatStart, int thatLength) {
        int thisArrayLen = getContentLength(this.bytes, this.start);
        int thatArrayLen = getContentLength(thatBytes, thatStart);

        int thisArrayStart = this.getContentStartOffset();
        int thatArrayStart = thatStart + getNumberBytesToStoreMeta(thatArrayLen);

        for (int thisIndex = 0, thatIndex = 0;
             thisIndex < thisArrayLen && thatIndex < thatArrayLen; ++thisIndex, ++thatIndex) {
            if (this.bytes[thisArrayStart + thisIndex] != thatBytes[thatArrayStart + thatIndex]) {
                return (0xff & this.bytes[thisArrayStart + thisIndex]) - (0xff & thatBytes[thatArrayStart + thatIndex]);
            }
        }
        return thisArrayLen - thatArrayLen;
    }

    public int getContentLength() {
        return contentLength;
    }

    public int getMetaLength() {
        return metaLength;
    }

    @Override
    public int hash() {
        if (hash == 0) {
            int h = 0;
            int realLength = getContentLength();
            int startOffset = getContentStartOffset();
            for (int i = 0; i < realLength; ++i) {
                h = 31 * h + bytes[startOffset + i];
            }
            hash = h;
        }
        return hash;
    }

    @Override
    public int getLength() {
        return getContentLength() + getMetaLength();
    }

    public int getContentStartOffset() {
        return getStartOffset() + getMetaLength();
    }

    ///////////////// helper functions ////////////////////////////////
    public static byte[] copyContent(ByteArrayPointable bytePtr) {
        return Arrays.copyOfRange(bytePtr.getByteArray(), bytePtr.getContentStartOffset(),
                bytePtr.getContentStartOffset() + bytePtr.getContentLength());
    }

    public static ByteArrayPointable generatePointableFromPureBytes(byte[] bytes) {
        return generatePointableFromPureBytes(bytes, 0, bytes.length);
    }

    public static ByteArrayPointable generatePointableFromPureBytes(byte[] bytes, int start, int length) {
        int metaLen = getNumberBytesToStoreMeta(length);
        byte[] ret = new byte[length + metaLen];
        VarLenIntEncoderDecoder.encode(length, ret, 0);
        for (int i = 0; i < length; ++i) {
            ret[i + metaLen] = bytes[start + i];
        }
        ByteArrayPointable ptr = new ByteArrayPointable();
        ptr.set(ret, 0, ret.length);
        return ptr;
    }

    public static int getContentLength(byte[] bytes, int offset) {
        return VarLenIntEncoderDecoder.decode(bytes, offset);
    }

    public static int getNumberBytesToStoreMeta(int length) {
        return VarLenIntEncoderDecoder.getBytesRequired(length);
    }

    /**
     * Compute the normalized key of the byte array.
     * The normalized key in Hyracks is mainly used to speedup the comparison between pointable data.
     * In the ByteArray case, we compute the integer value by using the first 4 bytes.
     * The comparator will first use this integer to get the result ( <,>, or =), it will check
     * the actual bytes only if the normalized key is equal. Thus this normalized key must be
     * consistent with the comparison result.
     *
     * @param bytesPtr
     * @param start
     * @return
     */
    public static int normalize(byte[] bytesPtr, int start) {
        int len = getContentLength(bytesPtr, start);
        long nk = 0;
        start = start + getNumberBytesToStoreMeta(len);
        for (int i = 0; i < 4; ++i) {
            nk <<= 8;
            if (i < len) {
                nk |= bytesPtr[start + i] & 0xff;
            }
        }
        return (int) (nk >> 1); // make it always positive.
    }

}
