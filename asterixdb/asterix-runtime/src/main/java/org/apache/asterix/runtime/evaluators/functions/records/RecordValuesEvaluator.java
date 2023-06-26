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
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.asterix.runtime.evaluators.functions.CastTypeEvaluator;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

class RecordValuesEvaluator implements IScalarEvaluator {

    private final IPointable inputRecordPointable = new VoidPointable();
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput resultOutput = resultStorage.getDataOutput();
    private final IScalarEvaluator eval0;
    private final boolean inputRecordOpen;
    private final ARecordPointable recordPointable;
    private final ArrayBackedValueStorage fieldValueStorage;
    private OrderedListBuilder listBuilder;
    private ARecordVisitablePointable openRecordPointable;
    private CastTypeEvaluator inputRecordCaster;

    RecordValuesEvaluator(IScalarEvaluator eval0, ARecordType recordType) {
        this.eval0 = eval0;
        if (recordType != null) {
            inputRecordOpen = recordType.isOpen() && recordType.getFieldTypes().length == 0;
            openRecordPointable = new ARecordVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
            inputRecordCaster = new CastTypeEvaluator(BuiltinType.ANY, recordType, eval0);
            listBuilder = new OrderedListBuilder();
        } else {
            inputRecordOpen = true;
        }
        if (inputRecordOpen) {
            recordPointable = new ARecordPointable();
            fieldValueStorage = new ArrayBackedValueStorage();
        } else {
            recordPointable = null;
            fieldValueStorage = null;
        }
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        eval0.evaluate(tuple, inputRecordPointable);

        if (PointableHelper.checkAndSetMissingOrNull(result, inputRecordPointable)) {
            return;
        }

        final ATypeTag inputTypeTag = PointableHelper.getTypeTag(inputRecordPointable);
        if (inputTypeTag != ATypeTag.OBJECT) {
            PointableHelper.setNull(result);
            return;
        }
        resultStorage.reset();
        if (inputRecordOpen) {
            buildOutputList();
        } else {
            buildOutputList(tuple);
        }
        result.set(resultStorage);
    }

    private void buildOutputList(IFrameTupleReference tuple) throws HyracksDataException {
        listBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
        inputRecordCaster.evaluate(tuple, inputRecordPointable);
        openRecordPointable.set(inputRecordPointable);
        final List<IVisitablePointable> fieldValues = openRecordPointable.getFieldValues();
        for (int i = 0, valuesCount = fieldValues.size(); i < valuesCount; i++) {
            listBuilder.addItem(fieldValues.get(i));
        }
        listBuilder.write(resultOutput, true);
    }

    private void buildOutputList() throws HyracksDataException {
        listBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
        recordPointable.set(inputRecordPointable);
        int openFieldCount = recordPointable.getOpenFieldCount(RecordUtil.FULLY_OPEN_RECORD_TYPE);
        for (int i = 0; i < openFieldCount; i++) {
            fieldValueStorage.reset();
            try {
                recordPointable.getOpenFieldValue(RecordUtil.FULLY_OPEN_RECORD_TYPE, i,
                        fieldValueStorage.getDataOutput());
                listBuilder.addItem(fieldValueStorage);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
        listBuilder.write(resultOutput, true);
    }
}
