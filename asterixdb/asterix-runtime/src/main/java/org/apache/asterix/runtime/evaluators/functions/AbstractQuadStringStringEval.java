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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractQuadStringStringEval implements IScalarEvaluator {

    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private DataOutput dout = resultStorage.getDataOutput();
    private IPointable array0 = new VoidPointable();
    private IPointable array1 = new VoidPointable();
    private IPointable array2 = new VoidPointable();
    private IPointable array3 = new VoidPointable();
    private IScalarEvaluator eval0;
    private IScalarEvaluator eval1;
    private IScalarEvaluator eval2;
    private IScalarEvaluator eval3;

    private final FunctionIdentifier funcID;

    private AMutableString resultBuffer = new AMutableString("");
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer strSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);

    private final UTF8StringPointable strPtr1st = new UTF8StringPointable();
    private final UTF8StringPointable strPtr2nd = new UTF8StringPointable();
    private final UTF8StringPointable strPtr3rd = new UTF8StringPointable();
    private final UTF8StringPointable strPtr4th = new UTF8StringPointable();

    public AbstractQuadStringStringEval(IHyracksTaskContext context, IScalarEvaluatorFactory eval0,
            IScalarEvaluatorFactory eval1, IScalarEvaluatorFactory eval2, IScalarEvaluatorFactory eval3,
            FunctionIdentifier funcID) throws AlgebricksException {
        this.eval0 = eval0.createScalarEvaluator(context);
        this.eval1 = eval1.createScalarEvaluator(context);
        this.eval2 = eval2.createScalarEvaluator(context);
        this.eval3 = eval3.createScalarEvaluator(context);
        this.funcID = funcID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
        eval0.evaluate(tuple, array0);
        eval1.evaluate(tuple, array1);
        eval2.evaluate(tuple, array2);
        eval3.evaluate(tuple, array3);

        resultStorage.reset();
        if (array0.getByteArray()[array0.getStartOffset()] != ATypeTag.SERIALIZED_STRING_TYPE_TAG
                || array1.getByteArray()[array1.getStartOffset()] != ATypeTag.SERIALIZED_STRING_TYPE_TAG
                || array2.getByteArray()[array2.getStartOffset()] != ATypeTag.SERIALIZED_STRING_TYPE_TAG
                || array3.getByteArray()[array3.getStartOffset()] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            throw new AlgebricksException(funcID.getName()
                    + ": expects input type (STRING/NULL, STRING/NULL, STRING/NULL, STRING/NULL), but got ("
                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array0.getByteArray()[array0.getStartOffset()])
                    + ", "
                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array1.getByteArray()[array1.getStartOffset()])
                    + ", "
                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array2.getByteArray()[array2.getStartOffset()])
                    + ", "
                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(array3.getByteArray()[array3.getStartOffset()])
                    + ".");
        }

        strPtr1st.set(array0.getByteArray(), array0.getStartOffset() + 1, array0.getLength());
        strPtr2nd.set(array1.getByteArray(), array1.getStartOffset() + 1, array1.getLength());
        strPtr3rd.set(array2.getByteArray(), array2.getStartOffset() + 1, array2.getLength());
        strPtr4th.set(array3.getByteArray(), array3.getStartOffset() + 1, array3.getLength());

        try {
            String res = compute(strPtr1st, strPtr2nd, strPtr3rd, strPtr4th);
            resultBuffer.setValue(res);
            strSerde.serialize(resultBuffer, dout);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
        result.set(resultStorage);
    }

    protected abstract String compute(UTF8StringPointable strPtr1st, UTF8StringPointable strPtr2nd,
            UTF8StringPointable strPtr3rd, UTF8StringPointable strPtr4th) throws IOException;

}
