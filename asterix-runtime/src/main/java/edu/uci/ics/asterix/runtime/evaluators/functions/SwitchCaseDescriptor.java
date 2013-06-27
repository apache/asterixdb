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

import java.io.IOException;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SwitchCaseDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SwitchCaseDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SWITCH_CASE;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {

        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                final ArrayBackedValueStorage condOut = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage caseOut = new ArrayBackedValueStorage();
                final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();

                final ICopyEvaluator[] evals = new ICopyEvaluator[args.length];
                // condition
                evals[0] = args[0].createEvaluator(condOut);
                // case value
                for (int i = 1; i < evals.length - 1; i += 2) {
                    evals[i] = args[i].createEvaluator(caseOut);
                }
                // case expression
                for (int i = 2; i < evals.length - 1; i += 2) {
                    evals[i] = args[i].createEvaluator(argOut);
                }
                // default expression
                evals[evals.length - 1] = args[evals.length - 1].createEvaluator(argOut);

                return new ICopyEvaluator() {

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            int n = args.length;
                            condOut.reset();
                            evals[0].evaluate(tuple);
                            for (int i = 1; i < n; i += 2) {
                                caseOut.reset();
                                evals[i].evaluate(tuple);
                                if (equals(condOut, caseOut)) {
                                    argOut.reset();
                                    evals[i + 1].evaluate(tuple);
                                    output.getDataOutput().write(argOut.getByteArray(), argOut.getStartOffset(),
                                            argOut.getLength());
                                    return;
                                }
                            }
                            // the default case
                            argOut.reset();
                            evals[n - 1].evaluate(tuple);
                            output.getDataOutput().write(argOut.getByteArray(), argOut.getStartOffset(),
                                    argOut.getLength());
                        } catch (HyracksDataException hde) {
                            throw new AlgebricksException(hde);
                        } catch (IOException ioe) {
                            throw new AlgebricksException(ioe);
                        }
                    }

                    private boolean equals(ArrayBackedValueStorage out1, ArrayBackedValueStorage out2) {
                        if (out1.getStartOffset() != out2.getStartOffset() || out1.getLength() != out2.getLength())
                            return false;
                        byte[] data1 = out1.getByteArray();
                        byte[] data2 = out2.getByteArray();
                        for (int i = out1.getStartOffset(); i < out1.getLength(); i++) {
                            if (data1[i] != data2[i]) {
                                return false;
                            }
                        }
                        return true;
                    }
                };
            }
        };
    }

}
