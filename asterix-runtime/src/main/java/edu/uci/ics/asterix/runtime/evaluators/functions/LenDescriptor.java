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

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
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

public class LenDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
    private final static byte SER_UNORDEREDLIST_TYPE_TAG = ATypeTag.UNORDEREDLIST.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new LenDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {

        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {

                return new ICopyEvaluator() {

                    private final DataOutput out = output.getDataOutput();
                    private final ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private final ICopyEvaluator evalList = args[0].createEvaluator(inputVal);

                    // result
                    private final AMutableInt32 res = new AMutableInt32(0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT32);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        inputVal.reset();
                        evalList.evaluate(tuple);

                        byte[] serList = inputVal.getByteArray();

                        if (serList[0] == SER_NULL_TYPE_TAG) {
                            try {
                                nullSerde.serialize(ANull.NULL, out);
                            } catch (HyracksDataException e) {
                                throw new AlgebricksException(e);
                            }
                            return;
                        }

                        if (serList[0] != SER_ORDEREDLIST_TYPE_TAG && serList[0] != SER_UNORDEREDLIST_TYPE_TAG) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.LEN.getName()
                                    + ": expects input type ORDEREDLIST/UNORDEREDLIST but got "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serList[0]));
                        }

                        int numberOfitems = 0;
                        if (serList[0] == SER_ORDEREDLIST_TYPE_TAG)
                            numberOfitems = AOrderedListSerializerDeserializer.getNumberOfItems(serList);
                        else
                            numberOfitems = AUnorderedListSerializerDeserializer.getNumberOfItems(serList);

                        res.setValue(numberOfitems);
                        try {
                            int32Serde.serialize(res, out);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.LEN;
    }

}
