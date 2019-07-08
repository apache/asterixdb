/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
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

public class UnorderedListConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new UnorderedListConstructorDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_EXPRESSION_TYPE;
        }
    };

    private static final long serialVersionUID = 1L;
    private AUnorderedListType ultype;

    @Override
    public void setImmutableStates(Object... states) {
        this.ultype = (AUnorderedListType) states[0];
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new UnorderedListConstructorEvaluatorFactory(args, ultype);
    }

    private static class UnorderedListConstructorEvaluatorFactory implements IScalarEvaluatorFactory {

        private static final long serialVersionUID = 1L;
        private IScalarEvaluatorFactory[] args;
        private boolean selfDescList = false;
        private boolean homoList = false;
        private AUnorderedListType unorderedlistType;

        public UnorderedListConstructorEvaluatorFactory(IScalarEvaluatorFactory[] args, AUnorderedListType type) {
            this.args = args;
            this.unorderedlistType = type;
            if (type == null || type.getItemType() == null || type.getItemType().getTypeTag() == ATypeTag.ANY) {
                this.selfDescList = true;
            } else {
                this.homoList = true;
            }
        }

        @Override
        public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
            final IPointable inputVal = new VoidPointable();
            final IScalarEvaluator[] argEvals = new IScalarEvaluator[args.length];
            for (int i = 0; i < args.length; i++) {
                argEvals[i] = args[i].createScalarEvaluator(ctx);
            }

            return new IScalarEvaluator() {
                private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                private DataOutput out = resultStorage.getDataOutput();
                private UnorderedListBuilder builder = new UnorderedListBuilder();

                @Override
                public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                    try {
                        builder.reset(unorderedlistType);
                        if (selfDescList) {
                            this.writeUntypedItems(tuple);
                        }
                        if (homoList) {
                            this.writeTypedItems(tuple);
                        }
                        resultStorage.reset();
                        builder.write(out, true);
                        result.set(resultStorage);
                    } catch (IOException ioe) {
                        throw HyracksDataException.create(ioe);
                    }
                }

                private void writeUntypedItems(IFrameTupleReference tuple) throws HyracksDataException {

                    try {
                        for (int i = 0; i < argEvals.length; i++) {
                            argEvals[i].evaluate(tuple, inputVal);
                            builder.addItem(inputVal);
                        }
                    } catch (IOException ioe) {
                        throw HyracksDataException.create(ioe);
                    }
                }

                private void writeTypedItems(IFrameTupleReference tuple) throws HyracksDataException {

                    try {
                        for (int i = 0; i < argEvals.length; i++) {
                            argEvals[i].evaluate(tuple, inputVal);
                            builder.addItem(inputVal);
                        }
                    } catch (IOException ioe) {
                        throw HyracksDataException.create(ioe);
                    }
                }

            };

        }
    }
}
