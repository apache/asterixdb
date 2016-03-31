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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

/**
 * This builder is used to build the variable length encoding object (e.g. UTF8String or ByteArray).
 * The caller needs to give an estimated length when {@link #reset(GrowableArray, int)}.
 * Then it can append the content byte by byte.
 * Since the actual byte length to store the content length is not precise at the beginning, the caller need
 * to explicitly call the {@link #finish()} function to notify that one object has finished building.
 * Then internally this builder will take care of storing the actual length field at the beginning of the
 * given storage array.
 */
public abstract class AbstractVarLenObjectBuilder {
    protected GrowableArray ary;
    protected DataOutput out;
    protected int startOffset;
    protected int estimateMetaLen;

    /**
     * Start to build an variable length object
     *
     * @param ary            the destination storage array
     * @param estimateLength the estimate length of this object
     * @throws IOException
     */
    public void reset(GrowableArray ary, int estimateLength) throws IOException {
        this.ary = ary;
        this.out = ary.getDataOutput();
        this.startOffset = ary.getLength();
        this.estimateMetaLen = VarLenIntEncoderDecoder.getBytesRequired(estimateLength);

        // increase the offset
        for (int i = 0; i < estimateMetaLen; i++) {
            out.writeByte(0);
        }
    }

    /**
     * Finish building an variable length object.
     * It will write the correct length of the object at the beginning of the storage array.
     * Since the actual byte size for storing the length could be changed ( if the given estimated length varies too
     * far from the actual length), we need to shift the data around in some cases.
     * Specifically, if the varlength(actual length) > varlength(estimated length) we need to grow the storage and
     * shift the content rightward. Else we need to shift the data leftward and tell the storage to rewind the
     * difference to mark the correct position.
     *
     * @throws IOException
     */
    public void finish() throws IOException {
        int actualDataLength = ary.getLength() - startOffset - estimateMetaLen;
        int actualMetaLen = VarLenIntEncoderDecoder.getBytesRequired(actualDataLength);
        if (actualMetaLen != estimateMetaLen) {// ugly but rare situation if the estimate vary a lot
            int diff = estimateMetaLen - actualMetaLen;
            int actualDataStart = startOffset + actualMetaLen;
            if (diff > 0) { // shrink
                for (int i = 0; i < actualDataLength; i++) {
                    ary.getByteArray()[actualDataStart + i] = ary.getByteArray()[actualDataStart + i + diff];
                }
                ary.rewindPositionBy(diff);
            } else { // increase space
                diff = -diff;
                for (int i = 0; i < diff; i++) {
                    out.writeByte(0);
                }
                for (int i = ary.getLength() - 1; i >= actualDataStart + diff; i--) {
                    ary.getByteArray()[i] = ary.getByteArray()[i - diff];
                }
            }
        }
        VarLenIntEncoderDecoder.encode(actualDataLength, ary.getByteArray(), startOffset);
    }

}
