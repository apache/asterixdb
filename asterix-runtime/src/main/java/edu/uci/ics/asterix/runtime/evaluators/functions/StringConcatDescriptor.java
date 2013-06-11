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

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.AsterixListAccessor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.util.StringUtils;

public class StringConcatDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private static final byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
    private static final byte SER_UNORDEREDLIST_TYPE_TAG = ATypeTag.UNORDEREDLIST.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringConcatDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private final AsterixListAccessor listAccessor = new AsterixListAccessor();
                    private final DataOutput out = output.getDataOutput();
                    private final ICopyEvaluatorFactory listEvalFactory = args[0];
                    private final ArrayBackedValueStorage outInputList = new ArrayBackedValueStorage();
                    private final ICopyEvaluator evalList = listEvalFactory.createEvaluator(outInputList);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            outInputList.reset();
                            evalList.evaluate(tuple);
                            byte[] listBytes = outInputList.getByteArray();
                            if (listBytes[0] != SER_ORDEREDLIST_TYPE_TAG && listBytes[0] != SER_UNORDEREDLIST_TYPE_TAG) {
                                throw new AlgebricksException(AsterixBuiltinFunctions.STRING_CONCAT.getName()
                                        + ": expects input type ORDEREDLIST/UNORDEREDLIST, but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(listBytes[0]));
                            }
                            try {
                                listAccessor.reset(listBytes, 0);
                            } catch (AsterixException e) {
                                throw new AlgebricksException(e);
                            }
                            try {
                                // calculate length first
                                int utf8Len = 0;
                                for (int i = 0; i < listAccessor.size(); i++) {
                                    int itemOffset = listAccessor.getItemOffset(i);
                                    ATypeTag itemType = listAccessor.getItemType(itemOffset);
                                    if (itemType != ATypeTag.STRING) {
                                        if (itemType == ATypeTag.NULL) {
                                            nullSerde.serialize(ANull.NULL, out);
                                            return;
                                        }
                                        throw new AlgebricksException(AsterixBuiltinFunctions.STRING_CONCAT.getName()
                                                + ": expects type STRING/NULL for the list item but got " + itemType);
                                    }
                                    utf8Len += UTF8StringPointable.getUTFLength(listBytes, itemOffset);
                                }
                                out.writeByte(ATypeTag.STRING.serialize());
                                StringUtils.writeUTF8Len(utf8Len, out);
                                for (int i = 0; i < listAccessor.size(); i++) {
                                    int itemOffset = listAccessor.getItemOffset(i);
                                    utf8Len = UTF8StringPointable.getUTFLength(listBytes, itemOffset);
                                    for (int j = 0; j < utf8Len; j++) {
                                        out.writeByte(listBytes[2 + itemOffset + j]);
                                    }
                                }
                            } catch (AsterixException ex) {
                                throw new AlgebricksException(ex);
                            }
                        } catch (IOException e1) {
                            throw new AlgebricksException(e1.getMessage());
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.STRING_CONCAT;
    }
}
