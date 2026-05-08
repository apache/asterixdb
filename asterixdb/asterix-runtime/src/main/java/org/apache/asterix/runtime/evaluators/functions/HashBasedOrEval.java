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

import java.io.DataOutput;

import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.comparisons.EqualsDescriptorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.BinaryEntry;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Hash-based evaluator for optimized OR expressions with multiple equality comparisons.
 *
 * <p>This evaluator optimizes expressions of the form:
 * <pre>
 *   field = const1 OR field = const2 OR field = const3 ...
 * </pre>
 * <p>A hash set is built containing all constant values.
 * We have to note that when getting to this evaluator, all the constants
 * should have compatible types, there for a case like "eq($a,1) or eq($a,null)"
 * or "eq($a,missing) or eq($a,3) or eq($a,1)" will not come through this evaluator
 * and will be using the original OrDescriptor.
 *
 * <h3>NULL and MISSING Handling</h3>
 * <p>Follows standard OR semantics where NULL and MISSING propagate to the result:
 * <ul>
 *   <li>If the field value is NULL → result is NULL</li>
 *   <li>If the field value is MISSING → result is MISSING</li>
 * </ul>
 *
 */

class HashBasedOrEval implements IScalarEvaluator {

    private static final byte[] DUMMY_VALUE = new byte[] { 0 };

    /** Same defaults as RecordAddFieldsDescriptor for BinaryHashMap. */
    private static final int TABLE_SIZE = 100;
    private static final int TABLE_FRAME_SIZE = 32768;

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput output = resultStorage.getDataOutput();
    private final VoidPointable valuePtr = new VoidPointable();
    private final VoidPointable constPtr = new VoidPointable();
    private final BinaryEntry keyEntry = new BinaryEntry();
    private final BinaryEntry valEntry = new BinaryEntry();
    private final BinaryHashMap valueSet;

    private final IBinaryHashFunction putHashFunc;
    private final IBinaryHashFunction getHashFunc;
    private final IBinaryComparator cmp;
    private final IAType elementType;

    private final IScalarEvaluator fieldEval;
    private final IScalarEvaluator[] constEvals;
    private final int numConst;
    private boolean setBuilt = false;

    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ABoolean> booleanSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ANull> nullSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AMissing> missingSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AMISSING);

    HashBasedOrEval(IAType elementType, IEvaluatorContext ctx, IScalarEvaluatorFactory[] args)
            throws HyracksDataException {
        this.elementType = elementType;
        this.numConst = args.length;

        EqualsDescriptorFactory firstEqFactory = (EqualsDescriptorFactory) args[0];
        fieldEval = firstEqFactory.getExpressionFactory().createScalarEvaluator(ctx);

        constEvals = new IScalarEvaluator[numConst];
        for (int i = 0; i < numConst; i++) {
            EqualsDescriptorFactory eqFactory = (EqualsDescriptorFactory) args[i];
            constEvals[i] = eqFactory.getConstantFactory().createScalarEvaluator(ctx);
        }
        putHashFunc = BinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(elementType)
                .createBinaryHashFunction();
        getHashFunc = BinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(elementType)
                .createBinaryHashFunction();
        cmp = BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(elementType, true)
                .createBinaryComparator();

        valueSet = new BinaryHashMap(TABLE_SIZE, TABLE_FRAME_SIZE, putHashFunc, getHashFunc, cmp);

    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        buildSetIfNeeded(tuple);

        fieldEval.evaluate(tuple, valuePtr);
        byte[] data = valuePtr.getByteArray();
        int offset = valuePtr.getStartOffset();

        if (data[offset] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
            nullSerde.serialize(ANull.NULL, output);
            result.set(resultStorage);
            return;
        }
        if (data[offset] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
            missingSerde.serialize(AMissing.MISSING, output);
            result.set(resultStorage);
            return;
        }
        if (!ATypeHierarchy.isCompatible(EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(data[offset]),
                elementType.getTypeTag())) {
            booleanSerde.serialize(ABoolean.FALSE, output);
            result.set(resultStorage);
            return;
        }

        keyEntry.set(data, offset, valuePtr.getLength());
        BinaryEntry found = valueSet.get(keyEntry);
        if (found == null) {
            booleanSerde.serialize(ABoolean.FALSE, output);
        } else {
            booleanSerde.serialize(ABoolean.TRUE, output);
        }
        result.set(resultStorage);
    }

    private void buildSetIfNeeded(IFrameTupleReference tuple) throws HyracksDataException {
        if (setBuilt) {
            return;
        }
        valEntry.set(DUMMY_VALUE, 0, 1);
        for (int i = 0; i < numConst; i++) {
            constEvals[i].evaluate(tuple, constPtr);
            byte[] data = constPtr.getByteArray();
            int offset = constPtr.getStartOffset();
            // the data here will never be of a Type NUll/missing
            // cases like "eq($a,null) or eq($a,1) or eq($a,5) "
            // will use the OrDiscriptor
            if (offset < data.length) {
                keyEntry.set(data, offset, constPtr.getLength());
                valueSet.put(keyEntry, valEntry);
            }
        }
        setBuilt = true;
    }
}
