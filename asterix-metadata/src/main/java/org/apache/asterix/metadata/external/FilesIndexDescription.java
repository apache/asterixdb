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
package org.apache.asterix.metadata.external;

import java.io.IOException;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.AqlTypeTraitProvider;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;

@SuppressWarnings("rawtypes")
public class FilesIndexDescription {
    public final static int FILE_INDEX_TUPLE_SIZE = 2;
    public final static int FILE_KEY_INDEX = 0;
    public final static int FILE_KEY_SIZE = 1;
    public final static int FILE_PAYLOAD_INDEX = 1;
    public final static String[] payloadFieldNames = { "FileName", "FileSize", "FileModDate" };
    public final static IAType[] payloadFieldTypes = { BuiltinType.ASTRING, BuiltinType.AINT64, BuiltinType.ADATETIME };

    public static final int[] BLOOM_FILTER_FIELDS = { 0 };
    public static final int EXTERNAL_FILE_NAME_FIELD_INDEX = 0;
    public static final int EXTERNAL_FILE_SIZE_FIELD_INDEX = 1;
    public static final int EXTERNAL_FILE_MOD_DATE_FIELD_INDEX = 2;

    public final ARecordType EXTERNAL_FILE_RECORD_TYPE;
    public final ITypeTraits[] EXTERNAL_FILE_BUDDY_BTREE_TYPE_TRAITS = new ITypeTraits[1];
    public final ITypeTraits[] EXTERNAL_FILE_INDEX_TYPE_TRAITS = new ITypeTraits[FILE_INDEX_TUPLE_SIZE];

    public final ISerializerDeserializer EXTERNAL_FILE_RECORD_SERDE;
    public final RecordDescriptor FILE_INDEX_RECORD_DESCRIPTOR;
    public final RecordDescriptor FILE_BUDDY_BTREE_RECORD_DESCRIPTOR;
    public final ISerializerDeserializer[] EXTERNAL_FILE_BUDDY_BTREE_FIELDS = new ISerializerDeserializer[1];
    public final ISerializerDeserializer[] EXTERNAL_FILE_TUPLE_FIELDS = new ISerializerDeserializer[FILE_INDEX_TUPLE_SIZE];
    public final IBinaryComparatorFactory[] FILES_INDEX_COMP_FACTORIES = new IBinaryComparatorFactory[] {
            AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(BuiltinType.AINT32, true) };

    public FilesIndexDescription() {
        ARecordType type;
        try {
            type = new ARecordType("ExternalFileRecordType", payloadFieldNames,
                    payloadFieldTypes, true);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        EXTERNAL_FILE_RECORD_TYPE = type;
        EXTERNAL_FILE_INDEX_TYPE_TRAITS[FILE_KEY_INDEX] = AqlTypeTraitProvider.INSTANCE
                .getTypeTrait(IndexingConstants.FILE_NUMBER_FIELD_TYPE);
        EXTERNAL_FILE_INDEX_TYPE_TRAITS[FILE_PAYLOAD_INDEX] = AqlTypeTraitProvider.INSTANCE
                .getTypeTrait(EXTERNAL_FILE_RECORD_TYPE);
        EXTERNAL_FILE_BUDDY_BTREE_TYPE_TRAITS[FILE_KEY_INDEX] = AqlTypeTraitProvider.INSTANCE
                .getTypeTrait(IndexingConstants.FILE_NUMBER_FIELD_TYPE);

        EXTERNAL_FILE_RECORD_SERDE = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(EXTERNAL_FILE_RECORD_TYPE);

        EXTERNAL_FILE_TUPLE_FIELDS[FILE_KEY_INDEX] = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(IndexingConstants.FILE_NUMBER_FIELD_TYPE);
        EXTERNAL_FILE_TUPLE_FIELDS[FILE_PAYLOAD_INDEX] = EXTERNAL_FILE_RECORD_SERDE;
        EXTERNAL_FILE_BUDDY_BTREE_FIELDS[FILE_KEY_INDEX] = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(IndexingConstants.FILE_NUMBER_FIELD_TYPE);

        FILE_INDEX_RECORD_DESCRIPTOR = new RecordDescriptor(EXTERNAL_FILE_TUPLE_FIELDS,
                EXTERNAL_FILE_INDEX_TYPE_TRAITS);

        FILE_BUDDY_BTREE_RECORD_DESCRIPTOR = new RecordDescriptor(EXTERNAL_FILE_BUDDY_BTREE_FIELDS,
                EXTERNAL_FILE_BUDDY_BTREE_TYPE_TRAITS);
    }

    @SuppressWarnings("unchecked")
    public void getBuddyBTreeTupleFromFileNumber(ArrayTupleReference tuple, ArrayTupleBuilder tupleBuilder,
            AMutableInt32 aInt32) throws IOException, AsterixException {
        tupleBuilder.reset();
        FILE_BUDDY_BTREE_RECORD_DESCRIPTOR.getFields()[0].serialize(aInt32,
                tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }
}
