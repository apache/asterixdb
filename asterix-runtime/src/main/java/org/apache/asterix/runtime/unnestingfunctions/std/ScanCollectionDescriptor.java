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

import java.io.DataOutput;
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
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopyUnnestingFunction;
import org.apache.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ScanCollectionDescriptor extends AbstractUnnestingFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ScanCollectionDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SCAN_COLLECTION;
    }

    @Override
    public ICopyUnnestingFunctionFactory createUnnestingFunctionFactory(final ICopyEvaluatorFactory[] args) {
        return new ScanCollectionUnnestingFunctionFactory(args[0]);
    }

    public static class ScanCollectionUnnestingFunctionFactory implements ICopyUnnestingFunctionFactory {

        private static final long serialVersionUID = 1L;
        private ICopyEvaluatorFactory listEvalFactory;

        public ScanCollectionUnnestingFunctionFactory(ICopyEvaluatorFactory arg) {
            this.listEvalFactory = arg;
        }

        @Override
        public ICopyUnnestingFunction createUnnestingFunction(IDataOutputProvider provider) throws AlgebricksException {

            final DataOutput out = provider.getDataOutput();

            return new ICopyUnnestingFunction() {

                private final AsterixListAccessor listAccessor = new AsterixListAccessor();
                private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                private ICopyEvaluator argEval = listEvalFactory.createEvaluator(inputVal);
                private int itemIndex;
                private boolean metNull = false;

                @Override
                public void init(IFrameTupleReference tuple) throws AlgebricksException {
                    try {
                        inputVal.reset();
                        argEval.evaluate(tuple);
                        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                                .deserialize(inputVal.getByteArray()[0]);
                        if (typeTag == ATypeTag.NULL) {
                            metNull = true;
                            return;
                        }
                        listAccessor.reset(inputVal.getByteArray(), 0);
                        itemIndex = 0;
                    } catch (AsterixException e) {
                        throw new AlgebricksException(e);
                    }
                }

                @Override
                public boolean step() throws AlgebricksException {
                    try {
                        if (!metNull) {
                            if (itemIndex < listAccessor.size()) {
                                listAccessor.writeItem(itemIndex, out);
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
