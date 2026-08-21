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
package org.apache.hyracks.dataflow.common.data.marshalling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.util.annotations.AiProvenance;

public class DoubleArraySerializerDeserializer implements ISerializerDeserializer<double[]> {
    private static final long serialVersionUID = 1L;

    public static final DoubleArraySerializerDeserializer INSTANCE = new DoubleArraySerializerDeserializer();

    private DoubleArraySerializerDeserializer() {
    }

    @Override
    public double[] deserialize(DataInput in) throws HyracksDataException {
        return read(in);
    }

    @Override
    public void serialize(double[] instance, DataOutput out) throws HyracksDataException {
        write(instance, out);
    }

    public static double[] read(DataInput in) throws HyracksDataException {
        try {
            int len = in.readInt();
            double[] array = new double[len];
            for (int i = 0; i < array.length; ++i) {
                array[i] = in.readDouble();
            }
            return array;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Decode an array written by {@link #write} directly out of a byte array, without routing through a
     * {@code DataInputStream}/{@code ByteArrayInputStream} pair.
     * <p>
     * The stream form costs a {@code synchronized} {@code read} call per element — {@code readDouble} ->
     * {@code readLong} -> {@code readFully} -> {@code ByteArrayInputStream.read(byte[],int,int)}, which JDK 1.0
     * made thread-safe and never revisited. Callers normally get away with it because a freshly allocated
     * stream never escapes and the JIT scalar-replaces it, eliding the lock; but that also means the
     * seemingly-cheaper trick of hoisting the stream into a reusable field is a large pessimization (measured
     * ~3x slower), since it forfeits escape analysis and, from JDK 18 on, has no biased locking to fall back
     * on. Decoding straight from the bytes sidesteps the question: no stream, no monitor, no allocation
     * beyond the result.
     *
     * @param bytes  buffer holding the serialized array
     * @param offset position of the leading length field
     * @param length number of bytes belonging to this field, used to validate the encoded length
     * @return the decoded array
     * @throws HyracksDataException if the encoded length does not fit within {@code length}
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Stream-free decode for hot per-record paths")
    public static double[] read(byte[] bytes, int offset, int length) throws HyracksDataException {
        int len = readLength(bytes, offset, length);
        double[] array = new double[len];
        decode(bytes, offset, array, len);
        return array;
    }

    /**
     * The element count of an array encoded at {@code offset}, without decoding any of it. Lets a caller
     * check the dimension before committing to a destination buffer.
     *
     * @throws HyracksDataException if the field is too short to hold the length prefix, or the encoded length
     *         does not fit within {@code length}
     */
    public static int readLength(byte[] bytes, int offset, int length) throws HyracksDataException {
        if (length < Integer.BYTES) {
            throw HyracksDataException.create(ErrorCode.MALFORMED_VECTOR_INDEX, "The encoded array field is " + length
                    + " bytes, but at least " + Integer.BYTES + " are needed for the length prefix");
        }
        int len = IntegerPointable.getInteger(bytes, offset);
        long required = (long) Integer.BYTES + (long) len * Double.BYTES;
        if (len < 0 || required > length) {
            throw HyracksDataException.create(ErrorCode.MALFORMED_VECTOR_INDEX, "The encoded array declares " + len
                    + " elements, needing " + required + " bytes, but the field is " + length + " bytes");
        }
        return len;
    }

    /**
     * Decode an encoded array into a caller-supplied destination, avoiding the allocation {@link #read} makes.
     * {@code dst.length} must equal the encoded element count — check it with {@link #readLength} first.
     *
     * @throws HyracksDataException if the encoding is malformed or {@code dst} is the wrong size
     */
    public static void readInto(byte[] bytes, int offset, int length, double[] dst) throws HyracksDataException {
        int len = readLength(bytes, offset, length);
        if (dst.length != len) {
            throw HyracksDataException.create(ErrorCode.UNEXPECTED_VECTOR_VALUE,
                    "Expected to see " + dst.length + " elements but the encoded vector has " + len);
        }
        decode(bytes, offset, dst, len);
    }

    private static void decode(byte[] bytes, int offset, double[] dst, int len) {
        int pos = offset + Integer.BYTES;
        for (int i = 0; i < len; i++, pos += Double.BYTES) {
            dst[i] = DoublePointable.getDouble(bytes, pos);
        }
    }

    public static void write(double[] instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeInt(instance.length);
            for (int i = 0; i < instance.length; ++i) {
                out.writeDouble(instance[i]);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
