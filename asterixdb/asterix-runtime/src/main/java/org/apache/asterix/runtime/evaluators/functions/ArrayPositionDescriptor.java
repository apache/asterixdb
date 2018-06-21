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

import org.apache.asterix.dataflow.data.nontagged.comparators.AObjectAscBinaryComparatorFactory;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ArrayPositionDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayPositionDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_POSITION;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
                return new ArrayPositionFunction(args, ctx);
            }
        };
    }

    public class ArrayPositionFunction implements IScalarEvaluator {
        private final ArrayBackedValueStorage storage;
        private final IPointable listArg;
        private final IPointable searchedValueArg;
        private final IScalarEvaluator listEval;
        private final IScalarEvaluator searchedValueEval;
        private final IBinaryComparator comp;
        private final ListAccessor listAccessor;
        private final AMutableInt32 intValue;
        private final ISerializerDeserializer intSerde;

        public ArrayPositionFunction(IScalarEvaluatorFactory[] args, IHyracksTaskContext ctx)
                throws HyracksDataException {
            storage = new ArrayBackedValueStorage();
            listArg = new VoidPointable();
            searchedValueArg = new VoidPointable();
            listEval = args[0].createScalarEvaluator(ctx);
            searchedValueEval = args[1].createScalarEvaluator(ctx);
            comp = AObjectAscBinaryComparatorFactory.INSTANCE.createBinaryComparator();
            listAccessor = new ListAccessor();
            intValue = new AMutableInt32(-1);
            intSerde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
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
                throw HyracksDataException.create(ErrorCode.CANNOT_COMPARE_COMPLEX, sourceLoc);
            }

            if (!ATYPETAGDESERIALIZER.deserialize(listBytes[listOffset]).isListType()) {
                PointableHelper.setNull(result);
                return;
            }

            listAccessor.reset(listBytes, listOffset);
            int numItems = listAccessor.size();
            intValue.setValue(-1);

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
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }

            storage.reset();
            intSerde.serialize(intValue, storage.getDataOutput());
            result.set(storage);
        }
    }
}
