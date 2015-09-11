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

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.*;

public class ByteArrayPointable extends AbstractPointable implements IHashable, IComparable {

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
    public int compareTo(byte[] bytes, int start, int length) {
        int thislen = getLength(this.bytes, this.start);
        int thatlen = getLength(bytes, start);

        for (int thisIndex = 0, thatIndex = 0; thisIndex < thislen && thatIndex < thatlen; ++thisIndex, ++thatIndex) {
            if (this.bytes[this.start + SIZE_OF_LENGTH + thisIndex] != bytes[start + SIZE_OF_LENGTH + thatIndex]) {
                return (0xff & this.bytes[this.start + SIZE_OF_LENGTH + thisIndex]) - (0xff & bytes[start + SIZE_OF_LENGTH
                        + thatIndex]);
            }
        }
        return thislen - thatlen;
    }

    @Override
    public int hash() {
        int h = 0;
        int realLength = getLength(bytes, start);
        for (int i = 0; i < realLength; ++i) {
            h = 31 * h + bytes[start + SIZE_OF_LENGTH + i];
        }
        return h;
    }

    @Override
    public int getLength(){
        return getFullLength(getByteArray(), getStartOffset());
    }

    public static final int SIZE_OF_LENGTH = 2;
    public static final int MAX_LENGTH = 65535;

    public static int getLength(byte[] bytes, int offset) {
        return ((0xFF & bytes[offset]) << 8) + (0xFF & bytes[offset + 1]);
    }

    public static int getFullLength(byte[] bytes, int offset){
        return getLength(bytes, offset) + SIZE_OF_LENGTH;
    }

    public static void putLength(int length, byte[] bytes, int offset) {
        bytes[offset] = (byte) ((length >>> 8) & 0xFF);
        bytes[offset + 1] = (byte) ((length >>> 0) & 0xFF);
    }

}
