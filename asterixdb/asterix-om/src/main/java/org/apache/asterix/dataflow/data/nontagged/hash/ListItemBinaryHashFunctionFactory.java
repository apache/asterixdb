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

package org.apache.asterix.dataflow.data.nontagged.hash;

import java.io.IOException;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.UTF8StringLowercasePointable;
import org.apache.hyracks.data.std.util.GrowableArray;

/**
 * This hash function factory is introduced to be able to hash heterogeneous list items.
 * The item type tag is also included in the hash computation to distinguish the different
 * types with the same raw bytes.
 */
public class ListItemBinaryHashFunctionFactory implements IBinaryHashFunctionFactory {

    private static final long serialVersionUID = 1L;

    public static final ListItemBinaryHashFunctionFactory INSTANCE = new ListItemBinaryHashFunctionFactory();

    private ListItemBinaryHashFunctionFactory() {
    }

    @Override
    public IBinaryHashFunction createBinaryHashFunction() {
        return createBinaryHashFunction(ATypeTag.ANY, false);
    }

    public IBinaryHashFunction createBinaryHashFunction(final ATypeTag itemTypeTag, final boolean ignoreCase) {
        return new IBinaryHashFunction() {

            private IBinaryHashFunction lowerCaseStringHash =
                    new PointableBinaryHashFunctionFactory(UTF8StringLowercasePointable.FACTORY)
                            .createBinaryHashFunction();
            private IBinaryHashFunction genericBinaryHash =
                    MurmurHash3BinaryHashFunctionFamily.INSTANCE.createBinaryHashFunction(0);
            private GrowableArray taggedBytes = new GrowableArray();

            @Override
            public int hash(byte[] bytes, int offset, int length) throws HyracksDataException {
                ATypeTag tag = itemTypeTag;
                int skip = 0;
                if (itemTypeTag == ATypeTag.ANY) {
                    tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
                    skip = 1;
                }
                switch (tag) {
                    case STRING: {
                        if (ignoreCase) {
                            return lowerCaseStringHash.hash(bytes, offset + skip, length - skip);
                        }
                    }
                    default: {
                        if (itemTypeTag != ATypeTag.ANY) {
                            // add the itemTypeTag in front of the data
                            try {
                                resetTaggedBytes(bytes, offset, length);
                                return genericBinaryHash.hash(taggedBytes.getByteArray(), 0, length + 1);
                            } catch (IOException e) {
                                throw HyracksDataException.create(e);
                            }
                        } else {
                            return genericBinaryHash.hash(bytes, offset, length);
                        }
                    }
                }
            }

            private void resetTaggedBytes(byte[] data, int offset, int length) throws IOException {
                taggedBytes.reset();
                taggedBytes.getDataOutput().writeByte(itemTypeTag.serialize());
                taggedBytes.getDataOutput().write(data, offset, length);
            }
        };
    }
}
