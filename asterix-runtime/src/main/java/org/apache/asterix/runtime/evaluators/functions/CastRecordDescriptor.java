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

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.cast.ACastVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class CastRecordDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CastRecordDescriptor();
        }
    };

    private static final long serialVersionUID = 1L;
    private ARecordType reqType;
    private ARecordType inputType;

    public void reset(ARecordType reqType, ARecordType inputType) {
        this.reqType = reqType;
        this.inputType = inputType;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.CAST_RECORD;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        final ICopyEvaluatorFactory recordEvalFactory = args[0];

        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                final DataOutput out = output.getDataOutput();
                final ArrayBackedValueStorage recordBuffer = new ArrayBackedValueStorage();
                final ICopyEvaluator recEvaluator = recordEvalFactory.createEvaluator(recordBuffer);

                return new ICopyEvaluator() {
                    // pointable allocator
                    private PointableAllocator allocator = new PointableAllocator();
                    final IVisitablePointable recAccessor = allocator.allocateRecordValue(inputType);
                    final IVisitablePointable resultAccessor = allocator.allocateRecordValue(reqType);
                    final ACastVisitor castVisitor = new ACastVisitor();
                    final Triple<IVisitablePointable, IAType, Boolean> arg = new Triple<IVisitablePointable, IAType, Boolean>(
                            resultAccessor, reqType, Boolean.FALSE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            recordBuffer.reset();
                            recEvaluator.evaluate(tuple);
                            recAccessor.set(recordBuffer);
                            recAccessor.accept(castVisitor, arg);
                            out.write(resultAccessor.getByteArray(), resultAccessor.getStartOffset(),
                                    resultAccessor.getLength());
                        } catch (Exception ioe) {
                            throw new AlgebricksException(ioe);
                        }
                    }
                };
            }
        };
    }
}
