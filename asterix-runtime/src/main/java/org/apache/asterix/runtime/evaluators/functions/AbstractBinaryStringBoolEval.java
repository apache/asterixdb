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

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractBinaryStringBoolEval implements IScalarEvaluator {

    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private DataOutput dout = resultStorage.getDataOutput();

    private IPointable ptr0 = new VoidPointable();
    private IPointable ptr1 = new VoidPointable();
    private IScalarEvaluator evalLeft;
    private IScalarEvaluator evalRight;
    private final FunctionIdentifier funcID;

    private final UTF8StringPointable leftPtr = new UTF8StringPointable();
    private final UTF8StringPointable rightPtr = new UTF8StringPointable();

    @SuppressWarnings({ "rawtypes" })
    private ISerializerDeserializer boolSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);

    public AbstractBinaryStringBoolEval(IHyracksTaskContext context, IScalarEvaluatorFactory evalLeftFactory,
            IScalarEvaluatorFactory evalRightFactory, FunctionIdentifier funcID) throws AlgebricksException {
        this.evalLeft = evalLeftFactory.createScalarEvaluator(context);
        this.evalRight = evalRightFactory.createScalarEvaluator(context);
        this.funcID = funcID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
        evalLeft.evaluate(tuple, ptr0);
        evalRight.evaluate(tuple, ptr1);

        byte[] bytes0 = ptr0.getByteArray();
        int offset0 = ptr0.getStartOffset();
        int len0 = ptr0.getLength();
        byte[] bytes1 = ptr1.getByteArray();
        int offset1 = ptr1.getStartOffset();
        int len1 = ptr1.getLength();

        resultStorage.reset();
        try {
            if (bytes0[offset0] == ATypeTag.SERIALIZED_NULL_TYPE_TAG
                    || bytes1[offset1] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                nullSerde.serialize(ANull.NULL, dout);
                result.set(resultStorage);
                return;
            } else if (bytes0[offset0] != ATypeTag.SERIALIZED_STRING_TYPE_TAG
                    || bytes1[offset1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                throw new AlgebricksException(funcID.getName() + ": expects input type STRING or NULL, but got "
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]) + " and "
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]) + ")!");
            }
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }

        leftPtr.set(bytes0, offset0 + 1, len0 - 1);
        rightPtr.set(bytes1, offset1 + 1, len1 - 1);

        ABoolean res = compute(leftPtr, rightPtr) ? ABoolean.TRUE : ABoolean.FALSE;
        try {
            boolSerde.serialize(res, dout);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
        result.set(resultStorage);
    }

    protected abstract boolean compute(UTF8StringPointable left, UTF8StringPointable right) throws AlgebricksException;

}
