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

package org.apache.asterix.runtime.unnestingfunctions.std;

import java.io.IOException;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.common.AsterixListAccessor;
import org.apache.asterix.runtime.unnestingfunctions.base.AbstractUnnestingFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
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
        return AsterixBuiltinFunctions.SCAN_COLLECTION;
    }

    @Override
    public IUnnestingEvaluatorFactory createUnnestingEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new ScanCollectionUnnestingFunctionFactory(args[0]);
    }

    public static class ScanCollectionUnnestingFunctionFactory implements IUnnestingEvaluatorFactory {

        private static final long serialVersionUID = 1L;
        private IScalarEvaluatorFactory listEvalFactory;

        public ScanCollectionUnnestingFunctionFactory(IScalarEvaluatorFactory arg) {
            this.listEvalFactory = arg;
        }

        @Override
        public IUnnestingEvaluator createUnnestingEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {

            return new IUnnestingEvaluator() {

                private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                private final AsterixListAccessor listAccessor = new AsterixListAccessor();
                private final IPointable inputVal = new VoidPointable();
                private final IScalarEvaluator argEval = listEvalFactory.createScalarEvaluator(ctx);
                private int itemIndex;
                private boolean metUnknown = false;

                @Override
                public void init(IFrameTupleReference tuple) throws AlgebricksException {
                    try {
                        metUnknown = false;
                        argEval.evaluate(tuple, inputVal);
                        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                                .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]);
                        if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
                            metUnknown = true;
                            return;
                        }
                        listAccessor.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                        itemIndex = 0;
                    } catch (AsterixException e) {
                        throw new AlgebricksException(e);
                    }
                }

                @Override
                public boolean step(IPointable result) throws AlgebricksException {
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
                        throw new AlgebricksException(e);
                    } catch (AsterixException e) {
                        throw new AlgebricksException(e);
                    }
                    return false;
                }

            };
        }

    }
}
