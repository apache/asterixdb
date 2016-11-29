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

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
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
    private ISerializerDeserializer strSerde = SerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);

    private final UTF8StringPointable strPtr1st = new UTF8StringPointable();
    private final UTF8StringPointable strPtr2nd = new UTF8StringPointable();
    private final UTF8StringPointable strPtr3rd = new UTF8StringPointable();
    private final UTF8StringPointable strPtr4th = new UTF8StringPointable();

    public AbstractQuadStringStringEval(IHyracksTaskContext context, IScalarEvaluatorFactory eval0,
            IScalarEvaluatorFactory eval1, IScalarEvaluatorFactory eval2, IScalarEvaluatorFactory eval3,
            FunctionIdentifier funcID) throws HyracksDataException {
        this.eval0 = eval0.createScalarEvaluator(context);
        this.eval1 = eval1.createScalarEvaluator(context);
        this.eval2 = eval2.createScalarEvaluator(context);
        this.eval3 = eval3.createScalarEvaluator(context);
        this.funcID = funcID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        eval0.evaluate(tuple, array0);
        eval1.evaluate(tuple, array1);
        eval2.evaluate(tuple, array2);
        eval3.evaluate(tuple, array3);

        byte[] bytes0 = array0.getByteArray();
        byte[] bytes1 = array1.getByteArray();
        byte[] bytes2 = array2.getByteArray();
        byte[] bytes3 = array3.getByteArray();

        int start0 = array0.getStartOffset();
        int start1 = array1.getStartOffset();
        int start2 = array2.getStartOffset();
        int start3 = array3.getStartOffset();

        int len0 = array0.getLength();
        int len1 = array1.getLength();
        int len2 = array2.getLength();
        int len3 = array3.getLength();

        resultStorage.reset();
        // Type check.
        if (bytes0[start0] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            throw new TypeMismatchException(funcID, 0, bytes0[start0], ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }
        if (bytes1[start1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            throw new TypeMismatchException(funcID, 1, bytes1[start1], ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }
        if (bytes2[start2] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            throw new TypeMismatchException(funcID, 2, bytes2[start2], ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }
        if (bytes3[start3] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            throw new TypeMismatchException(funcID, 3, bytes1[start3], ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }

        strPtr1st.set(bytes0, start0 + 1, len0);
        strPtr2nd.set(bytes1, start1 + 1, len1);
        strPtr3rd.set(bytes2, start2 + 1, len2);
        strPtr4th.set(bytes3, start3 + 1, len3);

        try {
            String res = compute(strPtr1st, strPtr2nd, strPtr3rd, strPtr4th);
            resultBuffer.setValue(res);
            strSerde.serialize(resultBuffer, dout);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        result.set(resultStorage);
    }

    protected abstract String compute(UTF8StringPointable strPtr1st, UTF8StringPointable strPtr2nd,
            UTF8StringPointable strPtr3rd, UTF8StringPointable strPtr4th) throws IOException;

}
