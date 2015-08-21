/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.asterix.runtime.evaluators.functions.binary;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABinary;
import edu.uci.ics.asterix.om.base.AMutableBinary;
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
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.ByteArrayBase64ParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.ByteArrayHexParserFactory;

public class ParseBinaryDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    static final String HEX_FORMAT = "hex";
    static final String BASE64_FORMAT = "base64";

    static boolean isCaseIgnoreEqual(String expectedLowerCaseString, byte[] bytes, int start,
            int length) {
        int pos = start;
        int index = 0;
        while (pos < start + length) {
            char c1 = UTF8StringPointable.charAt(bytes, pos);
            c1 = Character.toLowerCase(c1);
            if (expectedLowerCaseString.charAt(index++) != c1) {
                return false;
            }
            pos += UTF8StringPointable.charSize(bytes, pos);
        }
        return index == expectedLowerCaseString.length();
    }

    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ParseBinaryDescriptor();
        }
    };

    public static final ATypeTag[] EXPECTED_INPUT_TAGS = { ATypeTag.STRING, ATypeTag.STRING };

    @Override public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.PARSE_BINARY;
    }

    @Override public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            @Override public ICopyEvaluator createEvaluator(final IDataOutputProvider output)
                    throws AlgebricksException {
                return new AbstractCopyEvaluator(output, args) {

                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ABinary> binarySerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ABINARY);

                    private AMutableBinary aBinary = new AMutableBinary(new byte[0], 0, 0);
                    private final byte[] quadruplet = new byte[4];

                    @Override public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        ATypeTag binaryTag = evaluateTuple(tuple, 0);
                        ATypeTag formatTag = evaluateTuple(tuple, 1);

                        try {
                            if (serializeNullIfAnyNull(binaryTag, formatTag)) {
                                return;
                            }
                            checkTypeMachingThrowsIfNot(getIdentifier().getName(), EXPECTED_INPUT_TAGS, binaryTag,
                                    formatTag);
                            int lengthString = UTF8StringPointable.getUTFLength(storages[0].getByteArray(), 1);
                            int lengthFormat = UTF8StringPointable.getUTFLength(storages[1].getByteArray(), 1);
                            byte[] buffer;
                            if (isCaseIgnoreEqual(HEX_FORMAT, storages[1].getByteArray(), 3, lengthFormat)) {
                                buffer = ByteArrayHexParserFactory
                                        .extractPointableArrayFromHexString(storages[0].getByteArray(), 3,
                                                lengthString, aBinary.getBytes());
                            } else if (isCaseIgnoreEqual(BASE64_FORMAT, storages[1].getByteArray(), 3, lengthFormat)) {
                                buffer = ByteArrayBase64ParserFactory
                                        .extractPointableArrayFromBase64String(storages[0].getByteArray(), 3
                                                , lengthString, aBinary.getBytes(), quadruplet);
                            } else {
                                throw new AlgebricksException(getIdentifier().getName()
                                        + ": expects format indicator of \"hex\" or \"base64\" in the 2nd argument");
                            }
                            aBinary.setValue(buffer, 0, buffer.length);
                            binarySerde.serialize(aBinary, dataOutput);
                        } catch (HyracksDataException e) {
                            e.printStackTrace();
                        }
                    }
                };

            }

        };
    }

}
