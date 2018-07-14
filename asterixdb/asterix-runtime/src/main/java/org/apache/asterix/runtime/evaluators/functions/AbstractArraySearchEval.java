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
package org.apache.asterix.runtime.evaluators.functions;

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

import java.io.IOException;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.comparators.AObjectAscBinaryComparatorFactory;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractArraySearchEval implements IScalarEvaluator {
    private final IPointable listArg;
    private final IPointable searchedValueArg;
    private final IScalarEvaluator listEval;
    private final IScalarEvaluator searchedValueEval;
    private final IBinaryComparator comp;
    private final ListAccessor listAccessor;
    private final SourceLocation sourceLocation;
    protected final AMutableInt32 intValue;
    protected final ArrayBackedValueStorage storage;

    public AbstractArraySearchEval(IScalarEvaluatorFactory[] args, IHyracksTaskContext ctx, SourceLocation sourceLoc)
            throws HyracksDataException {
        storage = new ArrayBackedValueStorage();
        listArg = new VoidPointable();
        searchedValueArg = new VoidPointable();
        listEval = args[0].createScalarEvaluator(ctx);
        searchedValueEval = args[1].createScalarEvaluator(ctx);
        comp = AObjectAscBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        listAccessor = new ListAccessor();
        intValue = new AMutableInt32(-1);
        sourceLocation = sourceLoc;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        // 1st arg: list
        listEval.evaluate(tuple, listArg);
        byte[] listBytes = listArg.getByteArray();
        int listOffset = listArg.getStartOffset();

        // 2nd arg: value to search for
        searchedValueEval.evaluate(tuple, searchedValueArg);
        byte[] valueBytes = searchedValueArg.getByteArray();
        int valueOffset = searchedValueArg.getStartOffset();
        int valueLength = searchedValueArg.getLength();

        // for now, we don't support deep equality of object/lists. Throw an error if the value is of these types
        if (ATYPETAGDESERIALIZER.deserialize(valueBytes[valueOffset]).isDerivedType()) {
            throw new RuntimeDataException(ErrorCode.CANNOT_COMPARE_COMPLEX, sourceLocation);
        }

        if (!ATYPETAGDESERIALIZER.deserialize(listBytes[listOffset]).isListType()) {
            PointableHelper.setNull(result);
            return;
        }

        // initialize variables; -1 = value not found
        intValue.setValue(-1);
        listAccessor.reset(listBytes, listOffset);
        int numItems = listAccessor.size();

        try {
            for (int i = 0; i < numItems; i++) {
                storage.reset();
                listAccessor.writeItem(i, storage.getDataOutput());
                if (comp.compare(storage.getByteArray(), storage.getStartOffset(), storage.getLength(), valueBytes,
                        valueOffset, valueLength) == 0) {
                    intValue.setValue(i);
                    break;
                }
            }
            processResult(intValue, result);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected abstract void processResult(AMutableInt32 intValue, IPointable result) throws HyracksDataException;
}
