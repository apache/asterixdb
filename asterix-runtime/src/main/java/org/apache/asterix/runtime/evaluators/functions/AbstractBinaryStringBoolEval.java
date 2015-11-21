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
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractBinaryStringBoolEval implements ICopyEvaluator {

    private DataOutput dout;

    // allowed input types
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();

    private ArrayBackedValueStorage array0 = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage array1 = new ArrayBackedValueStorage();
    private ICopyEvaluator evalLeft;
    private ICopyEvaluator evalRight;
    private final FunctionIdentifier funcID;

    private final UTF8StringPointable leftPtr = new UTF8StringPointable();
    private final UTF8StringPointable rightPtr = new UTF8StringPointable();

    @SuppressWarnings({ "rawtypes" })
    private ISerializerDeserializer boolSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);

    public AbstractBinaryStringBoolEval(DataOutput dout, ICopyEvaluatorFactory evalLeftFactory,
            ICopyEvaluatorFactory evalRightFactory, FunctionIdentifier funcID) throws AlgebricksException {
        this.dout = dout;
        this.evalLeft = evalLeftFactory.createEvaluator(array0);
        this.evalRight = evalRightFactory.createEvaluator(array1);
        this.funcID = funcID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        array0.reset();
        evalLeft.evaluate(tuple);
        array1.reset();
        evalRight.evaluate(tuple);

        try {
            if (array0.getByteArray()[0] == SER_NULL_TYPE_TAG || array1.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                nullSerde.serialize(ANull.NULL, dout);
                return;
            } else if (array0.getByteArray()[0] != SER_STRING_TYPE_TAG
                    || array1.getByteArray()[0] != SER_STRING_TYPE_TAG) {
                throw new AlgebricksException(funcID.getName() + ": expects input type STRING or NULL, but got "
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array0.getByteArray()[0]) + " and "
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array1.getByteArray()[0]) + ")!");
            }
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }

        leftPtr.set(array0.getByteArray(), array0.getStartOffset() + 1, array0.getLength());
        rightPtr.set(array1.getByteArray(), array1.getStartOffset() + 1, array1.getLength());

        ABoolean res = compute(leftPtr, rightPtr) ? ABoolean.TRUE : ABoolean.FALSE;
        try {
            boolSerde.serialize(res, dout);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    protected abstract boolean compute(UTF8StringPointable left, UTF8StringPointable right) throws AlgebricksException;

}
