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

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Node;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Node metadata entity to an ITupleReference and vice versa.
 */
public class NodeTupleTranslator extends AbstractTupleTranslator<Node> {
    private static final long serialVersionUID = -5257435809246039182L;

    // Field indexes of serialized Node in a tuple.
    // First key field.
    public static final int NODE_NODENAME_TUPLE_FIELD_INDEX = 0;
    // Payload field containing serialized Node.
    public static final int NODE_PAYLOAD_TUPLE_FIELD_INDEX = 1;

    private transient AMutableInt64 aInt64 = new AMutableInt64(-1);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);

    // @SuppressWarnings("unchecked")
    // private ISerializerDeserializer<ARecord> recordSerDes =
    // NonTaggedSerializerDeserializerProvider.INSTANCE
    // .getSerializerDeserializer(recordType);

    protected NodeTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.NODE_DATASET.getFieldCount());
    }

    @Override
    public Node getMetadataEntityFromTuple(ITupleReference frameTuple) {
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
    public ITupleReference getTupleFromMetadataEntity(Node instance) throws HyracksDataException, AlgebricksException {
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
        aInt64.setValue(instance.getNumberOfCores());
        int64Serde.serialize(aInt64, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.NODE_ARECORD_NUMBEROFCORES_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aInt64.setValue(instance.getWorkingMemorySize());
        int64Serde.serialize(aInt64, fieldValue.getDataOutput());
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

        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
