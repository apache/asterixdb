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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.cast.ACastVisitor;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * This runtime function casts an input ADM instance of a certain type into the form
 * that confirms a required type.
 */
public class CastTypeDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CastTypeDescriptor();
        }
    };

    private static final long serialVersionUID = 1L;
    private IAType reqType;
    private IAType inputType;

    private CastTypeDescriptor() {
    }

    public void reset(IAType reqType, IAType inputType) {
        // If reqType or inputType is null, or they are the same, it indicates there is a bug in the compiler.
        if (reqType == null || inputType == null || reqType.equals(inputType)) {
            throw new IllegalStateException(
                    "Invalid types for casting, required type " + reqType + ", input type " + inputType);
        }
        // NULLs and MISSINGs are handled by the generated code, therefore we only need to handle actual types here.
        this.reqType = TypeComputeUtils.getActualType(reqType);
        this.inputType = TypeComputeUtils.getActualType(inputType);
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.CAST_TYPE;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        final IScalarEvaluatorFactory recordEvalFactory = args[0];

        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws HyracksDataException {
                return new CastTypeEvaluator(reqType, inputType, recordEvalFactory.createScalarEvaluator(ctx));
            }
        };
    }
}

class CastTypeEvaluator implements IScalarEvaluator {

    private final IScalarEvaluator argEvaluator;
    private final IPointable argPointable = new VoidPointable();

    private final PointableAllocator allocator = new PointableAllocator();
    private final IVisitablePointable inputPointable;
    private final IVisitablePointable resultPointable;

    private final ACastVisitor castVisitor = new ACastVisitor();
    private final Triple<IVisitablePointable, IAType, Boolean> arg;

    public CastTypeEvaluator(IAType reqType, IAType inputType, IScalarEvaluator argEvaluator)
            throws HyracksDataException {
        try {
            this.argEvaluator = argEvaluator;
            this.inputPointable = allocatePointable(inputType, reqType);
            this.resultPointable = allocatePointable(reqType, inputType);
            this.arg = new Triple<>(resultPointable, reqType, Boolean.FALSE);
        } catch (AsterixException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        try {
            argEvaluator.evaluate(tuple, argPointable);
            inputPointable.set(argPointable);
            inputPointable.accept(castVisitor, arg);
            result.set(resultPointable);
        } catch (Exception ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    // Allocates the result pointable.
    private final IVisitablePointable allocatePointable(IAType typeForPointable, IAType typeForOtherSide)
            throws AsterixException {
        if (!typeForPointable.equals(BuiltinType.ANY)) {
            return allocator.allocateFieldValue(typeForPointable);
        }
        return allocatePointableForAny(typeForOtherSide);
    }

    // Allocates an input or result pointable if the input or required type is ANY.
    private IVisitablePointable allocatePointableForAny(IAType type) {
        ATypeTag tag = type.getTypeTag();
        switch (tag) {
            case RECORD:
                return allocator.allocateFieldValue(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
            case ORDEREDLIST:
                return allocator.allocateFieldValue(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
            case UNORDEREDLIST:
                return allocator.allocateFieldValue(DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE);
            default:
                return allocator.allocateFieldValue(null);
        }
    }

}
