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
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.util.StringUtils;

public class CodePointToStringDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CodePointToStringDescriptor();
        }
    };
    private final static byte SER_ORDEREDLIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
    private final static byte SER_INT32_TYPE_TAG = ATypeTag.INT32.serialize();

    private final static byte[] currentUTF8 = new byte[6];
    private final byte stringTypeTag = ATypeTag.STRING.serialize();

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            private int codePointToUTF8(int c) {
                if (c < 0x80) {
                    currentUTF8[0] = (byte) (c & 0x7F /* mask 7 lsb: 0b1111111 */);
                    return 1;
                } else if (c < 0x0800) {
                    currentUTF8[0] = (byte) (c >> 6 & 0x1F | 0xC0);
                    currentUTF8[1] = (byte) (c & 0x3F | 0x80);
                    return 2;
                } else if (c < 0x010000) {
                    currentUTF8[0] = (byte) (c >> 12 & 0x0F | 0xE0);
                    currentUTF8[1] = (byte) (c >> 6 & 0x3F | 0x80);
                    currentUTF8[2] = (byte) (c & 0x3F | 0x80);
                    return 3;
                } else if (c < 0x200000) {
                    currentUTF8[0] = (byte) (c >> 18 & 0x07 | 0xF0);
                    currentUTF8[1] = (byte) (c >> 12 & 0x3F | 0x80);
                    currentUTF8[2] = (byte) (c >> 6 & 0x3F | 0x80);
                    currentUTF8[3] = (byte) (c & 0x3F | 0x80);
                    return 4;
                } else if (c < 0x4000000) {
                    currentUTF8[0] = (byte) (c >> 24 & 0x03 | 0xF8);
                    currentUTF8[1] = (byte) (c >> 18 & 0x3F | 0x80);
                    currentUTF8[2] = (byte) (c >> 12 & 0x3F | 0x80);
                    currentUTF8[3] = (byte) (c >> 6 & 0x3F | 0x80);
                    currentUTF8[4] = (byte) (c & 0x3F | 0x80);
                    return 5;
                } else if (c < 0x80000000) {
                    currentUTF8[0] = (byte) (c >> 30 & 0x01 | 0xFC);
                    currentUTF8[1] = (byte) (c >> 24 & 0x3F | 0x80);
                    currentUTF8[2] = (byte) (c >> 18 & 0x3F | 0x80);
                    currentUTF8[3] = (byte) (c >> 12 & 0x3F | 0x80);
                    currentUTF8[4] = (byte) (c >> 6 & 0x3F | 0x80);
                    currentUTF8[5] = (byte) (c & 0x3F | 0x80);
                    return 6;
                }
                return 0;
            }

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();
                    private ICopyEvaluatorFactory listEvalFactory = args[0];
                    private ArrayBackedValueStorage outInputList = new ArrayBackedValueStorage();
                    private ICopyEvaluator evalList = listEvalFactory.createEvaluator(outInputList);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            outInputList.reset();
                            evalList.evaluate(tuple);
                            byte[] serOrderedList = outInputList.getByteArray();
                            if (serOrderedList[0] != SER_ORDEREDLIST_TYPE_TAG
                                    && serOrderedList[1] != SER_INT32_TYPE_TAG) {
                                throw new AlgebricksException(AsterixBuiltinFunctions.CODEPOINT_TO_STRING.getName()
                                        + ": expects input type ORDEREDLIST but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(serOrderedList[0]));
                            }
                            int size = AOrderedListSerializerDeserializer.getNumberOfItems(serOrderedList);
                            try {
                                // calculate length first
                                int utf_8_len = 0;
                                for (int i = 0; i < size; i++) {
                                    int itemOffset = AOrderedListSerializerDeserializer
                                            .getItemOffset(serOrderedList, i);
                                    int codePoint = AInt32SerializerDeserializer.getInt(serOrderedList, itemOffset);
                                    utf_8_len += codePointToUTF8(codePoint);
                                }
                                out.writeByte(stringTypeTag);
                                StringUtils.writeUTF8Len(utf_8_len, out);
                                for (int i = 0; i < size; i++) {
                                    int itemOffset = AOrderedListSerializerDeserializer
                                            .getItemOffset(serOrderedList, i);
                                    int codePoint = AInt32SerializerDeserializer.getInt(serOrderedList, itemOffset);
                                    utf_8_len = codePointToUTF8(codePoint);
                                    for (int j = 0; j < utf_8_len; j++) {
                                        out.writeByte(currentUTF8[j]);
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
        return AsterixBuiltinFunctions.CODEPOINT_TO_STRING;
    }
}
