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

import static org.apache.asterix.om.pointables.base.DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;

import java.io.DataOutput;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.cast.ACastVisitor;
import org.apache.asterix.om.pointables.cast.CastResult;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ToObjectVarStrDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private ARecordType argType;
    private ARecordType outType;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ToObjectVarStrDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return new FunctionTypeInferers.ToObjectVarStrTypeInferer();
        }
    };

    @Override
    public void setImmutableStates(Object... states) {
        outType = (ARecordType) states[0];
        if (((IAType) states[1]).getTypeTag() == ATypeTag.OBJECT) {
            argType = (ARecordType) states[1];
        } else {
            argType = NESTED_OPEN_RECORD_TYPE;
        }
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new ToObjectVarStrEvaluator(ctx);
            }

            final class ToObjectVarStrEvaluator implements IScalarEvaluator {

                private final IPointable arg0 = new VoidPointable();
                private final ArrayBackedValueStorage emptyRecStorage = new ArrayBackedValueStorage();
                private final DataOutput emptyRecOut = emptyRecStorage.getDataOutput();
                private final IScalarEvaluator eval0;
                private final ARecordVisitablePointable argRec;
                private final boolean castRequired;
                private ACastVisitor castVisitor;
                private CastResult castResult;
                private boolean wroteEmpty;

                private ToObjectVarStrEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                    eval0 = args[0].createScalarEvaluator(ctx);
                    if (!TypeHelper.isFullyOpen(argType) && TypeHelper.isFullyOpen(outType)) {
                        castRequired = true;
                        argRec = new ARecordVisitablePointable(argType);
                        castVisitor = new ACastVisitor();
                        castResult = new CastResult(new VoidPointable(), NESTED_OPEN_RECORD_TYPE);
                    } else {
                        castRequired = false;
                        argRec = null;
                    }
                }

                @Override
                public void evaluate(IFrameTupleReference tuple, IPointable resultPointable)
                        throws HyracksDataException {
                    eval0.evaluate(tuple, arg0);
                    if (arg0.getByteArray()[arg0.getStartOffset()] == ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                        if (castRequired) {
                            argRec.set(arg0);
                            argRec.accept(castVisitor, castResult);
                            resultPointable.set(castResult.getOutPointable());
                        } else {
                            resultPointable.set(arg0);
                        }
                    } else {
                        writeEmpty();
                        resultPointable.set(emptyRecStorage);
                    }
                }

                private void writeEmpty() throws HyracksDataException {
                    if (!wroteEmpty) {
                        wroteEmpty = true;
                        emptyRecStorage.reset();
                        RecordBuilder openRecordBuilder = new RecordBuilder();
                        openRecordBuilder.reset(NESTED_OPEN_RECORD_TYPE);
                        openRecordBuilder.init();
                        openRecordBuilder.write(emptyRecOut, true);
                    }

                }
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.TO_OBJECT_VAR_STR;
    }
}
