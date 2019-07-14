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

import static org.apache.asterix.om.util.container.ObjectFactories.RECORD_FACTORY;
import static org.apache.asterix.om.util.container.ObjectFactories.STORAGE_FACTORY;
import static org.apache.asterix.om.util.container.ObjectFactories.VOID_FACTORY;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.common.ListAccessorUtil;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.om.pointables.nonvisitor.RecordField;
import org.apache.asterix.om.pointables.nonvisitor.SortedRecord;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.FloatToDoubleTypeConvertComputer;
import org.apache.asterix.om.types.hierachy.IntegerToDoubleTypeConvertComputer;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.MurmurHash3BinaryHash;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class AMurmurHash3BinaryHashFunctionFamily implements IBinaryHashFunctionFamily {

    private static final long serialVersionUID = 1L;
    private final IAType type;

    public AMurmurHash3BinaryHashFunctionFamily(IAType type) {
        this.type = type;
    }

    public static IBinaryHashFunction createBinaryHashFunction(IAType type, int seed) {
        return new GenericHashFunction(type, seed);
    }

    /**
     * The returned hash function is used to promote a numeric type to a DOUBLE numeric type to return same hash value
     * for the original numeric value, regardless of the numeric type. (e.g., h( int64("1") )  =  h( double("1.0") )
     *
     * @param seed seed to be used by the hash function created
     *
     * @return a generic hash function
     */
    @Override
    public IBinaryHashFunction createBinaryHashFunction(final int seed) {
        return new GenericHashFunction(type, seed);
    }

    private static final class GenericHashFunction implements IBinaryHashFunction {

        private final ArrayBackedValueStorage valueBuffer = new ArrayBackedValueStorage();
        private final DataOutput valueOut = valueBuffer.getDataOutput();
        private final IObjectPool<IPointable, Void> voidPointableAllocator = new ListObjectPool<>(VOID_FACTORY);
        private final IObjectPool<IMutableValueStorage, Void> storageAllocator = new ListObjectPool<>(STORAGE_FACTORY);
        private final IObjectPool<SortedRecord, ARecordType> recordPool = new ListObjectPool<>(RECORD_FACTORY);
        private final IAType type;
        private final int seed;

        private GenericHashFunction(IAType type, int seed) {
            this.type = type;
            this.seed = seed;
        }

        @Override
        public int hash(byte[] bytes, int offset, int length) throws HyracksDataException {
            return hash(type, bytes, offset, length);
        }

        private int hash(IAType type, byte[] bytes, int offset, int length) throws HyracksDataException {
            // if a numeric type is encountered, then we promote each numeric type to the DOUBLE type.
            valueBuffer.reset();
            ATypeTag sourceTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);

            switch (sourceTag) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    try {
                        IntegerToDoubleTypeConvertComputer.getInstance().convertType(bytes, offset + 1, length - 1,
                                valueOut);
                    } catch (IOException e) {
                        throw HyracksDataException.create(ErrorCode.NUMERIC_PROMOTION_ERROR, e.getMessage());
                    }
                    return MurmurHash3BinaryHash.hash(valueBuffer.getByteArray(), valueBuffer.getStartOffset(),
                            valueBuffer.getLength(), seed);
                case FLOAT:
                    try {
                        FloatToDoubleTypeConvertComputer.getInstance().convertType(bytes, offset + 1, length - 1,
                                valueOut);
                    } catch (IOException e) {
                        throw HyracksDataException.create(ErrorCode.NUMERIC_PROMOTION_ERROR, e.getMessage());
                    }
                    return MurmurHash3BinaryHash.hash(valueBuffer.getByteArray(), valueBuffer.getStartOffset(),
                            valueBuffer.getLength(), seed);
                case ARRAY:
                    try {
                        return hashArray(type, bytes, offset);
                    } catch (IOException e) {
                        throw HyracksDataException.create(e);
                    }
                case OBJECT:
                    return hashRecord(type, bytes, offset);
                case DOUBLE:
                default:
                    return MurmurHash3BinaryHash.hash(bytes, offset, length, seed);
            }
        }

        private int hashArray(IAType type, byte[] bytes, int offset) throws IOException {
            IAType arrayType = TypeComputeUtils.getActualTypeOrOpen(type, ATypeTag.ARRAY);
            IAType itemType = ((AbstractCollectionType) arrayType).getItemType();
            ATypeTag itemTag = itemType.getTypeTag();
            int numItems = AOrderedListSerializerDeserializer.getNumberOfItems(bytes, offset);
            int hash = seed;
            IPointable item = voidPointableAllocator.allocate(null);
            ArrayBackedValueStorage storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
            try {
                for (int i = 0; i < numItems; i++) {
                    ListAccessorUtil.getItem(bytes, offset, i, ATypeTag.ARRAY, itemTag, item, storage);
                    hash ^= hash(itemType, item.getByteArray(), item.getStartOffset(), item.getLength());
                }
            } finally {
                voidPointableAllocator.free(item);
                storageAllocator.free(storage);
            }

            return hash;
        }

        private int hashRecord(IAType type, byte[] bytes, int offset) throws HyracksDataException {
            ARecordType recordType = (ARecordType) TypeComputeUtils.getActualTypeOrOpen(type, ATypeTag.OBJECT);
            SortedRecord record = recordPool.allocate(recordType);
            IPointable fieldValue = voidPointableAllocator.allocate(null);
            // TODO(ali): this is not ideal. should be removed when tagged pointables are introduced
            ArrayBackedValueStorage storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
            try {
                record.reset(bytes, offset);
                int hash = seed;
                while (!record.isEmpty()) {
                    RecordField field = record.poll();
                    storage.reset();
                    record.getFieldValue(field, fieldValue, storage);
                    IAType fieldType = record.getFieldType(field);
                    hash ^= field.getName().hash() ^ hash(fieldType, fieldValue.getByteArray(),
                            fieldValue.getStartOffset(), fieldValue.getLength());
                }
                return hash;
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            } finally {
                recordPool.free(record);
                voidPointableAllocator.free(fieldValue);
                storageAllocator.free(storage);
            }
        }
    }
}
