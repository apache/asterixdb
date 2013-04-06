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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import edu.uci.ics.asterix.builders.UnorderedListBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.AUnorderedList;
import edu.uci.ics.asterix.om.base.IACursor;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a NodeGroup metadata entity to an ITupleReference and vice versa.
 */
public class NodeGroupTupleTranslator extends AbstractTupleTranslator<NodeGroup> {

    // Field indexes of serialized NodeGroup in a tuple.
    // First key field.
    public static final int NODEGROUP_NODEGROUPNAME_TUPLE_FIELD_INDEX = 0;
    // Payload field containing serialized NodeGroup.
    public static final int NODEGROUP_PAYLOAD_TUPLE_FIELD_INDEX = 1;

    private UnorderedListBuilder listBuilder = new UnorderedListBuilder();
    private ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
    private List<String> nodeNames;
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.NODEGROUP_RECORDTYPE);

    public NodeGroupTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.NODEGROUP_DATASET.getFieldCount());
    }

    @Override
    public NodeGroup getMetadataEntytiFromTuple(ITupleReference frameTuple) throws IOException {
        byte[] serRecord = frameTuple.getFieldData(NODEGROUP_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(NODEGROUP_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(NODEGROUP_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord nodeGroupRecord = (ARecord) recordSerDes.deserialize(in);
        String gpName = ((AString) nodeGroupRecord
                .getValueByPos(MetadataRecordTypes.NODEGROUP_ARECORD_GROUPNAME_FIELD_INDEX)).getStringValue();
        IACursor cursor = ((AUnorderedList) nodeGroupRecord
                .getValueByPos(MetadataRecordTypes.NODEGROUP_ARECORD_NODENAMES_FIELD_INDEX)).getCursor();
        List<String> nodeNames = new ArrayList<String>();
        while (cursor.next()) {
            nodeNames.add(((AString) cursor.get()).getStringValue());
        }
        return new NodeGroup(gpName, nodeNames);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(NodeGroup instance) throws IOException, MetadataException {
        // write the key in the first field of the tuple
        tupleBuilder.reset();
        aString.setValue(instance.getNodeGroupName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the second field of the tuple
        recordBuilder.reset(MetadataRecordTypes.NODEGROUP_RECORDTYPE);
        // write field 0
        fieldValue.reset();
        aString.setValue(instance.getNodeGroupName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.NODEGROUP_ARECORD_GROUPNAME_FIELD_INDEX, fieldValue);

        // write field 1
        listBuilder
                .reset((AUnorderedListType) MetadataRecordTypes.NODEGROUP_RECORDTYPE.getFieldTypes()[MetadataRecordTypes.NODEGROUP_ARECORD_NODENAMES_FIELD_INDEX]);
        this.nodeNames = instance.getNodeNames();
        for (String nodeName : this.nodeNames) {
            itemValue.reset();
            aString.setValue(nodeName);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.NODEGROUP_ARECORD_NODENAMES_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.NODEGROUP_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

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
