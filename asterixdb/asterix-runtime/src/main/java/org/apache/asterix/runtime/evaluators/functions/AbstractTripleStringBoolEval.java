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

public abstract class AbstractTripleStringBoolEval extends AbstractTripleStringEval {

    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer boolSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

    public AbstractTripleStringBoolEval(IEvaluatorContext context, IScalarEvaluatorFactory eval0,
            IScalarEvaluatorFactory eval1, IScalarEvaluatorFactory eval2, FunctionIdentifier funcID,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(context, eval0, eval1, eval2, funcID, sourceLoc);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void process(UTF8StringPointable first, UTF8StringPointable second, UTF8StringPointable thrid,
            IPointable result) throws HyracksDataException {
        ABoolean res = compute(first, second, thrid) ? ABoolean.TRUE : ABoolean.FALSE;
        boolSerde.serialize(res, dout);
        result.set(resultStorage);
    }

    /**
     * Computes a boolean value from three input strings.
     *
     * @param first
     *            , the first input argument.
     * @param second
     *            , the second input argument.
     * @param third
     *            , the second input argument.
     * @return a boolean value.
     * @throws HyracksDataException
     */
    protected abstract boolean compute(UTF8StringPointable first, UTF8StringPointable second, UTF8StringPointable third)
            throws HyracksDataException;
}
