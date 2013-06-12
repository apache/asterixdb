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

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class OrDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new OrDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.OR;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {

        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                // one temp. buffer re-used by all children
                final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
                final ICopyEvaluator[] evals = new ICopyEvaluator[args.length];
                for (int i = 0; i < evals.length; i++) {
                    evals[i] = args[i].createEvaluator(argOut);
                }

                return new ICopyEvaluator() {
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            int n = args.length;
                            boolean res = false;
                            boolean metNull = false;
                            for (int i = 0; i < n; i++) {
                                argOut.reset();
                                evals[i].evaluate(tuple);
                                if (argOut.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                                    metNull = true;
                                    continue;
                                }
                                boolean argResult = ABooleanSerializerDeserializer.getBoolean(argOut.getByteArray(), 1);
                                if (argResult == true) {
                                    res = true;
                                    break;
                                }
                            }
                            if (metNull) {
                                if (!res) {
                                    ABoolean aResult = ABoolean.FALSE;
                                    booleanSerde.serialize(aResult, output.getDataOutput());
                                } else
                                    nullSerde.serialize(ANull.NULL, output.getDataOutput());
                            } else {
                                ABoolean aResult = res ? (ABoolean.TRUE) : (ABoolean.FALSE);
                                booleanSerde.serialize(aResult, output.getDataOutput());
                            }
                        } catch (HyracksDataException hde) {
                            throw new AlgebricksException(hde);
                        }
                    }
                };
            }
        };
    }

}
