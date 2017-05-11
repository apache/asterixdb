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
package org.apache.asterix.external.indexing;

import java.io.IOException;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
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
    public static final int FILE_INDEX_TUPLE_SIZE = 2;
    public static final int FILE_KEY_INDEX = 0;
    public static final int FILE_KEY_SIZE = 1;
    public static final int FILE_PAYLOAD_INDEX = 1;
    private static final String[] payloadFieldNames = { "FileName", "FileSize", "FileModDate" };
    private static final IAType[] payloadFieldTypes =
            { BuiltinType.ASTRING, BuiltinType.AINT64, BuiltinType.ADATETIME };

    public static final int[] BLOOM_FILTER_FIELDS = { 0 };
    public static final int EXTERNAL_FILE_NAME_FIELD_INDEX = 0;
    public static final int EXTERNAL_FILE_SIZE_FIELD_INDEX = 1;
    public static final int EXTERNAL_FILE_MOD_DATE_FIELD_INDEX = 2;

    public static final ARecordType EXTERNAL_FILE_RECORD_TYPE =
            new ARecordType("ExternalFileRecordType", payloadFieldNames, payloadFieldTypes, true);
    public static final ITypeTraits[] EXTERNAL_FILE_BUDDY_BTREE_TYPE_TRAITS =
            new ITypeTraits[] { TypeTraitProvider.INSTANCE.getTypeTrait(IndexingConstants.FILE_NUMBER_FIELD_TYPE) };
    public static final ITypeTraits[] EXTERNAL_FILE_INDEX_TYPE_TRAITS =
            new ITypeTraits[] { TypeTraitProvider.INSTANCE.getTypeTrait(IndexingConstants.FILE_NUMBER_FIELD_TYPE),
                    TypeTraitProvider.INSTANCE.getTypeTrait(EXTERNAL_FILE_RECORD_TYPE) };
    public static final IBinaryComparatorFactory[] FILES_INDEX_COMP_FACTORIES = new IBinaryComparatorFactory[] {
            BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(BuiltinType.AINT32, true) };
    public static final ISerializerDeserializer FILE_NUMBER_SERDE =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(IndexingConstants.FILE_NUMBER_FIELD_TYPE);
    public static final ISerializerDeserializer[] EXTERNAL_FILE_BUDDY_BTREE_FIELDS =
            new ISerializerDeserializer[] { FILE_NUMBER_SERDE };
    public static final RecordDescriptor FILE_BUDDY_BTREE_RECORD_DESCRIPTOR =
            new RecordDescriptor(EXTERNAL_FILE_BUDDY_BTREE_FIELDS, EXTERNAL_FILE_BUDDY_BTREE_TYPE_TRAITS);

    private FilesIndexDescription() {
    }

    public static ISerializerDeserializer createExternalFileRecordSerde() {
        return SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(EXTERNAL_FILE_RECORD_TYPE);
    }

    public static ISerializerDeserializer[] createExternalFileTupleFieldsSerdes() {
        return new ISerializerDeserializer[] { FILE_NUMBER_SERDE, createExternalFileRecordSerde() };
    }

    public static RecordDescriptor createFileIndexRecordDescriptor() {
        return new RecordDescriptor(createExternalFileTupleFieldsSerdes(), EXTERNAL_FILE_INDEX_TYPE_TRAITS);
    }

    @SuppressWarnings("unchecked")
    public static void getBuddyBTreeTupleFromFileNumber(ArrayTupleReference tuple, ArrayTupleBuilder tupleBuilder,
            AMutableInt32 aInt32) throws IOException, AsterixException {
        tupleBuilder.reset();
        FILE_NUMBER_SERDE.serialize(aInt32, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }
}
