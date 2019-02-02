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

package org.apache.asterix.metadata.entitytupletranslators;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.Calendar;

import org.apache.asterix.external.api.IDataSourceAdapter;
import org.apache.asterix.external.dataset.adapter.AdapterIdentifier;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class DatasourceAdapterTupleTranslator extends AbstractTupleTranslator<DatasourceAdapter> {
    private static final long serialVersionUID = 6183434454125673504L;

    // Field indexes of serialized Adapter in a tuple.
    // First key field.
    public static final int ADAPTER_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Second key field.
    public static final int ADAPTER_NAME_TUPLE_FIELD_INDEX = 1;

    // Payload field containing serialized Adapter.
    public static final int ADAPTER_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = SerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.DATASOURCE_ADAPTER_RECORDTYPE);

    protected DatasourceAdapterTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET.getFieldCount());
    }

    @Override
    public DatasourceAdapter getMetadataEntityFromTuple(ITupleReference tuple)
            throws AlgebricksException, HyracksDataException {
        byte[] serRecord = tuple.getFieldData(ADAPTER_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = tuple.getFieldStart(ADAPTER_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = tuple.getFieldLength(ADAPTER_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord adapterRecord = recordSerDes.deserialize(in);
        return createAdapterFromARecord(adapterRecord);
    }

    private DatasourceAdapter createAdapterFromARecord(ARecord adapterRecord) {
        String dataverseName = ((AString) adapterRecord
                .getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_DATAVERSENAME_FIELD_INDEX))
                        .getStringValue();
        String adapterName =
                ((AString) adapterRecord.getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_NAME_FIELD_INDEX))
                        .getStringValue();
        String classname = ((AString) adapterRecord
                .getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_CLASSNAME_FIELD_INDEX)).getStringValue();
        IDataSourceAdapter.AdapterType adapterType = IDataSourceAdapter.AdapterType.valueOf(
                ((AString) adapterRecord.getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_TYPE_FIELD_INDEX))
                        .getStringValue());

        return new DatasourceAdapter(new AdapterIdentifier(dataverseName, adapterName), classname, adapterType);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(DatasourceAdapter adapter)
            throws HyracksDataException, AlgebricksException {
        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(adapter.getAdapterIdentifier().getNamespace());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(adapter.getAdapterIdentifier().getName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the third field of the tuple

        recordBuilder.reset(MetadataRecordTypes.DATASOURCE_ADAPTER_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(adapter.getAdapterIdentifier().getNamespace());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(adapter.getAdapterIdentifier().getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_NAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(adapter.getClassname());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_CLASSNAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(adapter.getType().name());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_TYPE_FIELD_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

}
