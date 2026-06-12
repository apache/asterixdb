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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Validates the position argument of an {@code INSERT INTO ... AT n} rewrite. The function takes the
 * already-resolved (i.e. negative-adjusted) position as {@code arg[0]} and the target array's length
 * as {@code arg[1]}. The position is returned unchanged so this call can be inlined wherever the
 * resolved position is used; if the position lies outside {@code [0, len]} a soft
 * {@link ErrorCode#UPDATE_INSERT_POSITION_OUT_OF_BOUNDS} warning is emitted. The list-type assertion
 * for the target array is left to the existing {@code check-list} call in the FROM clause; this
 * function only handles the position bounds check.
 */
public class CheckInsertPositionDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = CheckInsertPositionDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                final IScalarEvaluator posEval = args[0].createScalarEvaluator(ctx);
                final IScalarEvaluator lenEval = args[1].createScalarEvaluator(ctx);
                final IPointable posPtr = new VoidPointable();
                final IPointable lenPtr = new VoidPointable();
                return new IScalarEvaluator() {
                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        posEval.evaluate(tuple, posPtr);
                        lenEval.evaluate(tuple, lenPtr);
                        long position = ATypeHierarchy.getLongValue(getIdentifier().getName(), 0, posPtr.getByteArray(),
                                posPtr.getStartOffset());
                        long arrayLength = ATypeHierarchy.getLongValue(getIdentifier().getName(), 1,
                                lenPtr.getByteArray(), lenPtr.getStartOffset());
                        if ((position < 0 || position > arrayLength) && ctx.getWarningCollector().shouldWarn()) {
                            ctx.getWarningCollector().warn(Warning.of(sourceLoc,
                                    ErrorCode.UPDATE_INSERT_POSITION_OUT_OF_BOUNDS, position, arrayLength));
                        }
                        result.set(posPtr);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.CHECK_INSERT_POSITION;
    }
}
