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

package org.apache.asterix.runtime.evaluators.functions.binary;

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.AMutableBinary;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.parsers.ByteArrayBase64ParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.ByteArrayHexParserFactory;

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
