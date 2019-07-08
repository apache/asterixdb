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

import java.io.IOException;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public abstract class AbstractBinaryStringBoolEval extends AbstractBinaryStringEval {

    // For outputting results.
    @SuppressWarnings({ "rawtypes" })
    private ISerializerDeserializer boolSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

    public AbstractBinaryStringBoolEval(IEvaluatorContext context, IScalarEvaluatorFactory evalLeftFactory,
            IScalarEvaluatorFactory evalRightFactory, FunctionIdentifier funcID, SourceLocation sourceLoc)
            throws HyracksDataException {
        super(context, evalLeftFactory, evalRightFactory, funcID, sourceLoc);
    }

    @Override
    public void process(UTF8StringPointable leftPtr, UTF8StringPointable rightPtr, IPointable result)
            throws IOException {
        ABoolean res = compute(leftPtr, rightPtr) ? ABoolean.TRUE : ABoolean.FALSE;
        boolSerde.serialize(res, dataOutput);
        result.set(resultStorage);
    }

    /**
     * Computes a boolean value from two input strings.
     *
     * @param left
     *            , the first input argument.
     * @param right
     *            , the second input argument.
     * @return a boolean value.
     * @throws IOException
     */
    protected abstract boolean compute(UTF8StringPointable left, UTF8StringPointable right) throws IOException;

}
