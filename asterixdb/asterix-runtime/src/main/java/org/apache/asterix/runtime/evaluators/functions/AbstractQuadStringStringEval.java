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
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractQuadStringStringEval implements IScalarEvaluator {

    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private DataOutput dout = resultStorage.getDataOutput();
    private IPointable ptr0 = new VoidPointable();
    private IPointable ptr1 = new VoidPointable();
    private IPointable ptr2 = new VoidPointable();
    private IPointable ptr3 = new VoidPointable();
    private IScalarEvaluator eval0;
    private IScalarEvaluator eval1;
    private IScalarEvaluator eval2;
    private IScalarEvaluator eval3;

    protected final FunctionIdentifier funcID;
    protected final SourceLocation sourceLoc;

    private AMutableString resultBuffer = new AMutableString("");
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer strSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);

    private final IEvaluatorContext ctx;
    private final UTF8StringPointable strPtr0 = new UTF8StringPointable();
    private final UTF8StringPointable strPtr1 = new UTF8StringPointable();
    private final UTF8StringPointable strPtr2 = new UTF8StringPointable();
    private final UTF8StringPointable strPtr3 = new UTF8StringPointable();

    public AbstractQuadStringStringEval(IEvaluatorContext context, IScalarEvaluatorFactory eval0,
            IScalarEvaluatorFactory eval1, IScalarEvaluatorFactory eval2, IScalarEvaluatorFactory eval3,
            FunctionIdentifier funcID, SourceLocation sourceLoc) throws HyracksDataException {
        this.ctx = context;
        this.eval0 = eval0.createScalarEvaluator(context);
        this.eval1 = eval1.createScalarEvaluator(context);
        this.eval2 = eval2.createScalarEvaluator(context);
        this.eval3 = eval3.createScalarEvaluator(context);
        this.funcID = funcID;
        this.sourceLoc = sourceLoc;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        eval0.evaluate(tuple, ptr0);
        eval1.evaluate(tuple, ptr1);
        eval2.evaluate(tuple, ptr2);
        eval3.evaluate(tuple, ptr3);

        if (PointableHelper.checkAndSetMissingOrNull(result, ptr0, ptr1, ptr2, ptr3)) {
            return;
        }

        if (!processArgument(0, ptr0, strPtr0) || !processArgument(1, ptr1, strPtr1)
                || !processArgument(2, ptr2, strPtr2) || !processArgument(3, ptr3, strPtr3)) {
            PointableHelper.setNull(result);
            return;
        }

        resultStorage.reset();
        try {
            String res = compute(strPtr0, strPtr1, strPtr2, strPtr3);
            resultBuffer.setValue(res);
            strSerde.serialize(resultBuffer, dout);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    protected boolean processArgument(int argIdx, IPointable argPtr, UTF8StringPointable outStrPtr)
            throws HyracksDataException {
        byte[] bytes = argPtr.getByteArray();
        int start = argPtr.getStartOffset();
        // Type check.
        if (bytes[start] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funcID, bytes[start], argIdx, ATypeTag.STRING);
            return false;
        }
        int len = argPtr.getLength();
        outStrPtr.set(bytes, start + 1, len);
        return true;
    }

    protected abstract String compute(UTF8StringPointable strPtr1st, UTF8StringPointable strPtr2nd,
            UTF8StringPointable strPtr3rd, UTF8StringPointable strPtr4th) throws IOException;
}
