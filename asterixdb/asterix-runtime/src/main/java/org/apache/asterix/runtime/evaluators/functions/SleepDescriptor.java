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
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
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

public class SleepDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger();

    public static final IFunctionDescriptorFactory FACTORY = SleepDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private IPointable argTime = new VoidPointable();
                    private final IScalarEvaluator evalValue = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator evalTime = args[1].createScalarEvaluator(ctx);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        evalTime.evaluate(tuple, argTime);

                        final byte[] bytes = argTime.getByteArray();
                        final int offset = argTime.getStartOffset();
                        final long time = ATypeHierarchy.getLongValue(getIdentifier().getName(), 1, bytes, offset);

                        try {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.log(Level.INFO,
                                        ctx.getTaskContext().getTaskAttemptId() + " sleeping for " + time + " ms");
                            }
                            Thread.sleep(time);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.log(Level.INFO,
                                        ctx.getTaskContext().getTaskAttemptId() + " done sleeping for " + time + " ms");
                            }
                        }

                        evalValue.evaluate(tuple, result);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SLEEP;
    }

}
