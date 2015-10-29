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
import org.apache.asterix.om.base.AMutableString;
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

public abstract class AbstractTripleStringStringEval implements ICopyEvaluator {

    private DataOutput dout;
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private ArrayBackedValueStorage array0 = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage array1 = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage array2 = new ArrayBackedValueStorage();
    private ICopyEvaluator eval0;
    private ICopyEvaluator eval1;
    private ICopyEvaluator eval2;

    private AMutableString resultBuffer = new AMutableString("");
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer strSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);

    private final UTF8StringPointable strPtr1st = new UTF8StringPointable();
    private final UTF8StringPointable strPtr2nd = new UTF8StringPointable();
    private final UTF8StringPointable strPtr3rd = new UTF8StringPointable();

    private final FunctionIdentifier funcID;

    public AbstractTripleStringStringEval(DataOutput dout, ICopyEvaluatorFactory eval0, ICopyEvaluatorFactory eval1,
            ICopyEvaluatorFactory eval2, FunctionIdentifier funcID) throws AlgebricksException {
        this.dout = dout;
        this.eval0 = eval0.createEvaluator(array0);
        this.eval1 = eval1.createEvaluator(array1);
        this.eval2 = eval2.createEvaluator(array2);
        this.funcID = funcID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        array0.reset();
        eval0.evaluate(tuple);
        array1.reset();
        eval1.evaluate(tuple);
        array2.reset();
        eval2.evaluate(tuple);

        try {
            if (array0.getByteArray()[0] == SER_NULL_TYPE_TAG || array1.getByteArray()[0] == SER_NULL_TYPE_TAG
                    || array2.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                nullSerde.serialize(ANull.NULL, dout);
                return;
            } else if (array0.getByteArray()[0] != SER_STRING_TYPE_TAG
                    || array1.getByteArray()[0] != SER_STRING_TYPE_TAG
                    || array2.getByteArray()[0] != SER_STRING_TYPE_TAG) {
                throw new AlgebricksException(funcID.getName()
                        + ": expects input type (STRING/NULL, STRING/NULL, STRING/NULL), but got ("
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array0.getByteArray()[0]) + ", "
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array1.getByteArray()[0]) + ", "
                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array2.getByteArray()[0]) + ".");
            }
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }

        strPtr1st.set(array0.getByteArray(), array0.getStartOffset() + 1, array0.getLength());
        strPtr2nd.set(array1.getByteArray(), array1.getStartOffset() + 1, array1.getLength());
        strPtr3rd.set(array2.getByteArray(), array2.getStartOffset() + 1, array2.getLength());

        String res = compute(strPtr1st, strPtr2nd, strPtr3rd);
        resultBuffer.setValue(res);
        try {
            strSerde.serialize(resultBuffer, dout);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    protected abstract String compute(UTF8StringPointable strPtr1st, UTF8StringPointable strPtr2nd,
            UTF8StringPointable strPtr3rd) throws AlgebricksException;
}
