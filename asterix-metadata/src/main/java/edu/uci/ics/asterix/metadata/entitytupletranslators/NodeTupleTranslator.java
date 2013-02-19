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

import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.entities.Node;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Node metadata entity to an ITupleReference and vice versa.
 */
public class NodeTupleTranslator extends AbstractTupleTranslator<Node> {

    // Field indexes of serialized Node in a tuple.
    // First key field.
    public static final int NODE_NODENAME_TUPLE_FIELD_INDEX = 0;
    // Payload field containing serialized Node.
    public static final int NODE_PAYLOAD_TUPLE_FIELD_INDEX = 1;

    private AMutableInt32 aInt32 = new AMutableInt32(-1);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT32);

    // @SuppressWarnings("unchecked")
    // private ISerializerDeserializer<ARecord> recordSerDes =
    // NonTaggedSerializerDeserializerProvider.INSTANCE
    // .getSerializerDeserializer(recordType);

    public NodeTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.NODE_DATASET.getFieldCount());
    }

    @Override
    public Node getMetadataEntytiFromTuple(ITupleReference frameTuple) throws IOException {
        throw new NotImplementedException();
        // TODO: Implement this.
        // try {
        // byte[] serRecord =
        // frameTuple.getFieldData(MetadataUtils.DUMMY_FIELD_NUMBER);
        // int recordStartOffset = frameTuple.getFieldStart(1);
        // int recordLength = frameTuple.getFieldLength(1);
        // ByteArrayInputStream stream = new ByteArrayInputStream(serRecord,
        // recordStartOffset, recordLength);
        // DataInput in = new DataInputStream(stream);
        // ARecord rec = (ARecord) recordSerDes.deserialize(in);
        // String nodeName = ((AString) rec.getValueByPos(0)).getStringValue();
        // int numOfCores = ((AInt32) rec.getValueByPos(1)).getIntegerValue();
        // int workMemSize = ((AInt32) rec.getValueByPos(2)).getIntegerValue();
        // IACursor cursor = ((AOrderedList) rec.getValueByPos(3)).getCursor();
        // List<String> nodeStores = new ArrayList<String>();
        // while (cursor.next())
        // nodeStores.add(((AString) cursor.get()).getStringValue());
        // return new Node(nodeName, numOfCores, workMemSize, nodeStores);
        // } catch (IOException e) {
        // throw new AlgebricksException(e);
        // }
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Node instance) throws IOException, MetadataException {
        // write the key in the first field of the tuple
        tupleBuilder.reset();
        aString.setValue(instance.getNodeName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the second field of the tuple
        recordBuilder.reset(MetadataRecordTypes.NODE_RECORDTYPE);
        // write field 0
        fieldValue.reset();
        aString.setValue(instance.getNodeName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.NODE_ARECORD_NODENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aInt32.setValue(instance.getNumberOfCores());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.NODE_ARECORD_NUMBEROFCORES_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aInt32.setValue(instance.getWorkingMemorySize());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.NODE_ARECORD_WORKINGMEMORYSIZE_FIELD_INDEX, fieldValue);

        // write field 3
        // listBuilder.reset((AOrderedListType)
        // recordType.getFieldTypes()[3]);
        // this.stores = instance.getStores();
        // for (String field : this.stores) {
        // itemValue.reset();
        // aString.setValue(field);
        // stringSerde.serialize(aString, itemValue.getDataOutput());
        // listBuilder.addItem(itemValue);
        // }
        // fieldValue.reset();
        // listBuilder.write(fieldValue.getDataOutput());
        // recordBuilder.addField(3, fieldValue);

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
