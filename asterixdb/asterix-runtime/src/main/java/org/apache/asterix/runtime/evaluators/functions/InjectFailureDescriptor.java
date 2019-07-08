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
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InjectFailureDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new InjectFailureDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.INJECT_FAILURE;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {

                final IPointable argPtr = new VoidPointable();
                final IScalarEvaluator[] evals = new IScalarEvaluator[args.length];
                evals[0] = args[0].createScalarEvaluator(ctx);
                evals[1] = args[1].createScalarEvaluator(ctx);

                return new IScalarEvaluator() {

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        evals[1].evaluate(tuple, argPtr);
                        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                                .deserialize(argPtr.getByteArray()[argPtr.getStartOffset()]);
                        if (typeTag == ATypeTag.BOOLEAN) {
                            boolean argResult = ABooleanSerializerDeserializer.getBoolean(argPtr.getByteArray(),
                                    argPtr.getStartOffset() + 1);
                            if (argResult) {
                                LOGGER.log(Level.ERROR, ctx.getTaskContext().getTaskAttemptId() + " injecting failure");
                                throw new RuntimeDataException(ErrorCode.INJECTED_FAILURE, sourceLoc,
                                        getIdentifier().getName());
                            }
                        }

                        // evaluate the real evaluator
                        evals[0].evaluate(tuple, argPtr);
                        result.set(argPtr.getByteArray(), argPtr.getStartOffset(), argPtr.getLength());
                    }

                };
            }
        };
    }
}
