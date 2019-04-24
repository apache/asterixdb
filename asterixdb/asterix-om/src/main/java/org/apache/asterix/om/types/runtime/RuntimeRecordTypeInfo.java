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
package org.apache.asterix.om.types.runtime;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.hyracks.util.string.UTF8StringWriter;

/**
 * This class contains the mutable states for a record type
 * so as to allow a caller to check whether a field name
 * is in the closed part of the record type.
 * The RuntimeRecordTypeInfo has to be one-per-partition
 * to avoid race conditions.
 */
public class RuntimeRecordTypeInfo {

    private final IBinaryHashFunction fieldNameHashFunction;
    private final IBinaryComparator fieldNameComparator;
    private final UTF8StringWriter writer;
    private final ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
    private final DataOutputStream dos = new DataOutputStream(baaos);

    private int[] serializedFieldNameOffsets;
    private long[] hashCodeIndexPairs;
    private ARecordType cachedRecType = null;

    public RuntimeRecordTypeInfo() {
        fieldNameComparator = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        fieldNameHashFunction =
                new PointableBinaryHashFunctionFactory(UTF8StringPointable.FACTORY).createBinaryHashFunction();
        writer = new UTF8StringWriter();
    }

    /**
     * Reset the binary artifacts of a runtime type info instance.
     *
     * @param recType,
     *            the record type.
     */
    public void reset(ARecordType recType) {
        if (cachedRecType == recType) {
            // if the type doesn't change, we just skip the reset.
            return;
        }
        // Sets the record type.
        cachedRecType = recType;
        // Resets the bytes for names.
        baaos.reset();

        if (recType != null) {
            String[] fieldNames = recType.getFieldNames();
            if (serializedFieldNameOffsets == null || serializedFieldNameOffsets.length != fieldNames.length) {
                serializedFieldNameOffsets = new int[fieldNames.length];
                hashCodeIndexPairs = new long[fieldNames.length];
            }
            int length = 0;
            try {
                for (int i = 0; i < fieldNames.length; ++i) {
                    serializedFieldNameOffsets[i] = baaos.size();
                    writer.writeUTF8(fieldNames[i], dos);
                    length = baaos.size() - serializedFieldNameOffsets[i];
                    hashCodeIndexPairs[i] =
                            fieldNameHashFunction.hash(baaos.getByteArray(), serializedFieldNameOffsets[i], length);
                    hashCodeIndexPairs[i] = hashCodeIndexPairs[i] << 32;
                    hashCodeIndexPairs[i] = hashCodeIndexPairs[i] | i;
                }
                dos.flush();
                Arrays.sort(hashCodeIndexPairs);
                for (int i = 0; i < fieldNames.length; i++) {
                    int j = getFieldIndex(baaos.getByteArray(), serializedFieldNameOffsets[i],
                            UTF8StringUtil.getStringLength(baaos.getByteArray(), serializedFieldNameOffsets[i]));
                    if (j != i) {
                        throw new RuntimeDataException(ErrorCode.DUPLICATE_FIELD_NAME, fieldNames[i]);
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        } else {
            serializedFieldNameOffsets = new int[0];
            hashCodeIndexPairs = new long[0];
        }
    }

    /**
     * Returns the position of the field in the closed schema or -1 if the field does not exist.
     *
     * @param bytes
     *            the serialized bytes of the field name
     * @param start
     *            the starting offset of the field name in bytes
     * @param length
     *            the length of the field name in bytes
     * @return the position of the field in the closed schema or -1 if the field does not exist.
     * @throws HyracksDataException
     */
    public int getFieldIndex(byte[] bytes, int start, int length) throws HyracksDataException {
        if (hashCodeIndexPairs.length == 0) {
            return -1;
        }
        int fIndex;
        int probeFieldHash = fieldNameHashFunction.hash(bytes, start, length);
        int i = Arrays.binarySearch(hashCodeIndexPairs, ((long) probeFieldHash) << 32);
        i = (i < 0) ? -1 * (i + 1) : i;

        while (i < hashCodeIndexPairs.length && (int) (hashCodeIndexPairs[i] >>> 32) == probeFieldHash) {
            fIndex = (int) hashCodeIndexPairs[i];
            int cFieldLength = UTF8StringUtil.getStringLength(baaos.getByteArray(), serializedFieldNameOffsets[fIndex]);
            if (fieldNameComparator.compare(baaos.getByteArray(), serializedFieldNameOffsets[fIndex], cFieldLength,
                    bytes, start, length) == 0) {
                return fIndex;
            }
            i++;
        }
        return -1;
    }

}
