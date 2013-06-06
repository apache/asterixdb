/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.runtime.unnestingfunctions.std;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.common.AsterixListAccessor;
import edu.uci.ics.asterix.runtime.unnestingfunctions.base.AbstractUnnestingFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

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
