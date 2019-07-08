/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.runtime.unnestingfunctions.std;

import java.io.IOException;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.asterix.runtime.unnestingfunctions.base.AbstractUnnestingFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ScanCollectionDescriptor extends AbstractUnnestingFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ScanCollectionDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SCAN_COLLECTION;
    }

    @Override
    public IUnnestingEvaluatorFactory createUnnestingEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new ScanCollectionUnnestingFunctionFactory(args[0], sourceLoc);
    }

    public static class ScanCollectionUnnestingFunctionFactory implements IUnnestingEvaluatorFactory {

        private static final long serialVersionUID = 1L;
        private IScalarEvaluatorFactory listEvalFactory;
        private final SourceLocation sourceLoc;

        public ScanCollectionUnnestingFunctionFactory(IScalarEvaluatorFactory arg, SourceLocation sourceLoc) {
            this.listEvalFactory = arg;
            this.sourceLoc = sourceLoc;
        }

        @Override
        public IUnnestingEvaluator createUnnestingEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
            return new IUnnestingEvaluator() {
                private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                private final ListAccessor listAccessor = new ListAccessor();
                private final IPointable inputVal = new VoidPointable();
                private final IScalarEvaluator argEval = listEvalFactory.createScalarEvaluator(ctx);
                private int itemIndex;
                private boolean metUnknown = false;

                @Override
                public void init(IFrameTupleReference tuple) throws HyracksDataException {
                    metUnknown = false;
                    argEval.evaluate(tuple, inputVal);
                    byte typeTag = inputVal.getByteArray()[inputVal.getStartOffset()];
                    if (typeTag == ATypeTag.SERIALIZED_MISSING_TYPE_TAG
                            || typeTag == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                        metUnknown = true;
                        return;
                    }
                    if (typeTag != ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG
                            && typeTag != ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
                        throw new TypeMismatchException(sourceLoc, typeTag, ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG,
                                ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG);
                    }
                    listAccessor.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                    itemIndex = 0;
                }

                @Override
                public boolean step(IPointable result) throws HyracksDataException {
                    try {
                        if (!metUnknown) {
                            if (itemIndex < listAccessor.size()) {
                                resultStorage.reset();
                                listAccessor.writeItem(itemIndex, resultStorage.getDataOutput());
                                result.set(resultStorage);
                                ++itemIndex;
                                return true;
                            }
                        }
                    } catch (IOException e) {
                        throw HyracksDataException.create(e);
                    }
                    return false;
                }
            };
        }

    }
}
