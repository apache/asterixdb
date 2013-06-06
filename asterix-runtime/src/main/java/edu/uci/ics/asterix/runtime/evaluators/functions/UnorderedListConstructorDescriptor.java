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
package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.UnorderedListBuilder;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class UnorderedListConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new UnorderedListConstructorDescriptor();
        }
    };

    private AUnorderedListType ultype;

    public void reset(AUnorderedListType unorderedListType) {
        this.ultype = unorderedListType;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new UnorderedListConstructorEvaluatorFactory(args, ultype);
    }

    private static class UnorderedListConstructorEvaluatorFactory implements ICopyEvaluatorFactory {

        private static final long serialVersionUID = 1L;
        private ICopyEvaluatorFactory[] args;

        private boolean selfDescList = false;
        private boolean homoList = false;
        private AUnorderedListType unorderedlistType;

        public UnorderedListConstructorEvaluatorFactory(ICopyEvaluatorFactory[] args, AUnorderedListType type) {
            this.args = args;
            this.unorderedlistType = type;
            if (type == null || type.getItemType() == null || type.getItemType().getTypeTag() == ATypeTag.ANY)
                this.selfDescList = true;
            else
                this.homoList = true;
        }

        @Override
        public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
            final DataOutput out = output.getDataOutput();
            final ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
            final ICopyEvaluator[] argEvals = new ICopyEvaluator[args.length];
            for (int i = 0; i < args.length; i++) {
                argEvals[i] = args[i].createEvaluator(inputVal);
            }

            return new ICopyEvaluator() {

                private UnorderedListBuilder builder = new UnorderedListBuilder();

                @Override
                public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                    try {
                        builder.reset(unorderedlistType);
                        if (selfDescList) {
                            this.writeUntypedItems(tuple);
                        }
                        if (homoList) {
                            this.writeTypedItems(tuple);
                        }
                        builder.write(out, true);
                    } catch (IOException ioe) {
                        throw new AlgebricksException(ioe);
                    }
                }

                private void writeUntypedItems(IFrameTupleReference tuple) throws AlgebricksException {

                    try {
                        for (int i = 0; i < argEvals.length; i++) {
                            inputVal.reset();
                            argEvals[i].evaluate(tuple);
                            builder.addItem(inputVal);
                        }
                    } catch (IOException ioe) {
                        throw new AlgebricksException(ioe);
                    }
                }

                private void writeTypedItems(IFrameTupleReference tuple) throws AlgebricksException {

                    try {
                        for (int i = 0; i < argEvals.length; i++) {
                            inputVal.reset();
                            argEvals[i].evaluate(tuple);
                            builder.addItem(inputVal);
                        }
                    } catch (IOException ioe) {
                        throw new AlgebricksException(ioe);
                    }
                }

            };

        }
    }

}
