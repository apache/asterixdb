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

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public final class IfMissingOrNullDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = IfMissingOrNullDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractIfMissingOrNullEval(ctx, args) {
                    @Override
                    protected boolean skip(byte argTypeTag) {
                        return argTypeTag == ATypeTag.SERIALIZED_MISSING_TYPE_TAG
                                || argTypeTag == ATypeTag.SERIALIZED_NULL_TYPE_TAG;
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.IF_MISSING_OR_NULL;
    }

    public static abstract class AbstractIfMissingOrNullEval implements IScalarEvaluator {

        private final IScalarEvaluator[] argEvals;

        private final IPointable argPtr;

        AbstractIfMissingOrNullEval(IEvaluatorContext ctx, IScalarEvaluatorFactory[] args) throws HyracksDataException {
            argEvals = new IScalarEvaluator[args.length];
            for (int i = 0; i < argEvals.length; i++) {
                argEvals[i] = args[i].createScalarEvaluator(ctx);
            }
            argPtr = new VoidPointable();
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            for (IScalarEvaluator argEval : argEvals) {
                argEval.evaluate(tuple, argPtr);
                byte[] bytes = argPtr.getByteArray();
                int offset = argPtr.getStartOffset();
                if (!skip(bytes[offset])) {
                    result.set(argPtr);
                    return;
                }
            }
            PointableHelper.setNull(result);
        }

        protected abstract boolean skip(byte argTypeTag);
    }
}
