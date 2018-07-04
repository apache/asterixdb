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

package org.apache.asterix.runtime.evaluators.functions.records;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

class RecordPairsEvaluator extends AbstractRecordPairsEvaluator {

    // For writing output list
    private final OrderedListBuilder listBuilder = new OrderedListBuilder();

    // For writing each individual output record.
    private final ArrayBackedValueStorage itemStorage = new ArrayBackedValueStorage();
    private final DataOutput itemOutput = itemStorage.getDataOutput();
    private final RecordBuilder recBuilder = new RecordBuilder();

    // Sets up the constant field names, "name" for the key field, "value" for the value field.
    private final ArrayBackedValueStorage nameStorage = new ArrayBackedValueStorage();
    private final ArrayBackedValueStorage valueStorage = new ArrayBackedValueStorage();

    private ARecordVisitablePointable recordVisitablePointable;

    RecordPairsEvaluator(IScalarEvaluator eval0, ARecordType recordType) throws HyracksDataException {
        super(eval0, recordType);
        recBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        recordVisitablePointable = new ARecordVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);

        try {
            AObjectSerializerDeserializer serde = AObjectSerializerDeserializer.INSTANCE;
            serde.serialize(new AString("name"), nameStorage.getDataOutput());
            serde.serialize(new AString("value"), valueStorage.getDataOutput());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    protected void buildOutput() throws HyracksDataException {
        listBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);

        recordVisitablePointable.set(inputPointable);
        List<IVisitablePointable> fieldNames = recordVisitablePointable.getFieldNames();
        List<IVisitablePointable> fieldValues = recordVisitablePointable.getFieldValues();
        // Adds each field of the input record as a key-value pair into the result.
        int numFields = recordVisitablePointable.getFieldNames().size();
        for (int fieldIndex = 0; fieldIndex < numFields; ++fieldIndex) {
            itemStorage.reset();
            recBuilder.init();
            recBuilder.addField(nameStorage, fieldNames.get(fieldIndex));
            recBuilder.addField(valueStorage, fieldValues.get(fieldIndex));
            recBuilder.write(itemOutput, true);
            listBuilder.addItem(itemStorage);
        }

        // Writes the result and sets the result pointable.
        listBuilder.write(resultOutput, true);
    }

    @Override
    protected boolean validateInputType(ATypeTag inputTypeTag) {
        return inputTypeTag == ATypeTag.OBJECT;
    }
}
