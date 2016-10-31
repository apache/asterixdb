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
import java.io.IOException;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class OrderedListConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new OrderedListConstructorDescriptor();
        }
    };

    private AOrderedListType oltype;

    public void reset(AOrderedListType orderedListType) {
        this.oltype = orderedListType;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.ORDERED_LIST_CONSTRUCTOR;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new OrderedListConstructorEvaluatorFactory(args, oltype);
    }

    private static class OrderedListConstructorEvaluatorFactory implements IScalarEvaluatorFactory {

        private static final long serialVersionUID = 1L;
        private IScalarEvaluatorFactory[] args;

        private boolean selfDescList = false;

        private AOrderedListType orderedlistType;

        public OrderedListConstructorEvaluatorFactory(IScalarEvaluatorFactory[] args, AOrderedListType type) {
            this.args = args;

            this.orderedlistType = type;
            if (type == null || type.getItemType() == null || type.getItemType().getTypeTag() == ATypeTag.ANY) {
                this.selfDescList = true;
            }
        }

        @Override
        public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws HyracksDataException {
            final IScalarEvaluator[] argEvals = new IScalarEvaluator[args.length];
            for (int i = 0; i < args.length; i++) {
                argEvals[i] = args[i].createScalarEvaluator(ctx);
            }

            return new IScalarEvaluator() {
                private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                private final DataOutput out = resultStorage.getDataOutput();
                private final IPointable inputVal = new VoidPointable();
                private final OrderedListBuilder builder = new OrderedListBuilder();

                @Override
                public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                    try {
                        resultStorage.reset();
                        builder.reset(orderedlistType);
                        if (selfDescList) {
                            this.writeUntypedItems(tuple);
                        } else {
                            this.writeTypedItems(tuple);
                        }
                        builder.write(out, true);
                        result.set(resultStorage);
                    } catch (IOException ioe) {
                        throw new HyracksDataException(ioe);
                    }
                }

                private void writeUntypedItems(IFrameTupleReference tuple) throws HyracksDataException {

                    try {
                        for (int i = 0; i < argEvals.length; i++) {
                            argEvals[i].evaluate(tuple, inputVal);
                            builder.addItem(inputVal);
                        }

                    } catch (IOException ioe) {
                        throw new HyracksDataException(ioe);
                    }
                }

                private void writeTypedItems(IFrameTupleReference tuple) throws HyracksDataException {

                    try {
                        for (int i = 0; i < argEvals.length; i++) {
                            argEvals[i].evaluate(tuple, inputVal);
                            builder.addItem(inputVal);
                        }

                    } catch (IOException ioe) {
                        throw new HyracksDataException(ioe);
                    }
                }

            };

        }

    }
}
