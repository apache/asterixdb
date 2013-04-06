/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.metadata.entitytupletranslators;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Calendar;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.dataset.adapter.AdapterIdentifier;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter;
import edu.uci.ics.asterix.metadata.entities.DatasourceAdapter.AdapterType;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class DatasourceAdapterTupleTranslator extends AbstractTupleTranslator<DatasourceAdapter> {

    // Field indexes of serialized Adapter in a tuple.
    // First key field.
    public static final int ADAPTER_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Second key field.
    public static final int ADAPTER_NAME_TUPLE_FIELD_INDEX = 1;

    // Payload field containing serialized Adapter.
    public static final int ADAPTER_PAYLOAD_TUPLE_FIELD_INDEX = 2;

    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.DATASOURCE_ADAPTER_RECORDTYPE);

    public DatasourceAdapterTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.DATASOURCE_ADAPTER_DATASET.getFieldCount());
    }

    @Override
    public DatasourceAdapter getMetadataEntytiFromTuple(ITupleReference tuple) throws MetadataException, IOException {
        byte[] serRecord = tuple.getFieldData(ADAPTER_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = tuple.getFieldStart(ADAPTER_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = tuple.getFieldLength(ADAPTER_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord adapterRecord = (ARecord) recordSerDes.deserialize(in);
        return createAdapterFromARecord(adapterRecord);
    }

    private DatasourceAdapter createAdapterFromARecord(ARecord adapterRecord) {
        String dataverseName = ((AString) adapterRecord
                .getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_DATAVERSENAME_FIELD_INDEX))
                .getStringValue();
        String adapterName = ((AString) adapterRecord
                .getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_NAME_FIELD_INDEX)).getStringValue();
        String classname = ((AString) adapterRecord
                .getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_CLASSNAME_FIELD_INDEX)).getStringValue();
        AdapterType adapterType = AdapterType.valueOf(((AString) adapterRecord
                .getValueByPos(MetadataRecordTypes.DATASOURCE_ADAPTER_ARECORD_TYPE_FIELD_INDEX)).getStringValue());

        return new DatasourceAdapter(new AdapterIdentifier(dataverseName, adapterName), classname, adapterType);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(DatasourceAdapter adapter) throws IOException, MetadataException {
        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(adapter.getAdapterIdentifier().getNamespace());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(adapter.getAdapterIdentifier().getAdapterName());
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
        aString.setValue(adapter.getAdapterIdentifier().getAdapterName());
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
        try {
            recordBuilder.write(tupleBuilder.getDataOutput(), true);
        } catch (AsterixException e) {
            throw new MetadataException(e);
        }
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

}
