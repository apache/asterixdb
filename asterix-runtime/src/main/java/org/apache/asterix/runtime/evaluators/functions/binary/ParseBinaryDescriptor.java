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
import org.apache.hyracks.util.bytes.Base64Parser;
import org.apache.hyracks.util.bytes.HexParser;

public class ParseBinaryDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    static final UTF8StringPointable HEX_FORMAT = UTF8StringPointable.generateUTF8Pointable("hex");
    static final UTF8StringPointable BASE64_FORMAT = UTF8StringPointable.generateUTF8Pointable("base64");

    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ParseBinaryDescriptor();
        }
    };

    public static final ATypeTag[] EXPECTED_INPUT_TAGS = { ATypeTag.STRING, ATypeTag.STRING };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.PARSE_BINARY;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output)
                    throws AlgebricksException {
                return new AbstractCopyEvaluator(output, args) {

                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ABinary> binarySerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ABINARY);

                    private AMutableBinary aBinary = new AMutableBinary(new byte[0], 0, 0);
                    private final UTF8StringPointable stringPointable = new UTF8StringPointable();
                    private final UTF8StringPointable formatPointable = new UTF8StringPointable();

                    private final HexParser hexParser = new HexParser();
                    private final Base64Parser base64Parser = new Base64Parser();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        ATypeTag binaryTag = evaluateTuple(tuple, 0);
                        ATypeTag formatTag = evaluateTuple(tuple, 1);

                        try {
                            if (serializeNullIfAnyNull(binaryTag, formatTag)) {
                                return;
                            }
                            checkTypeMachingThrowsIfNot(getIdentifier().getName(), EXPECTED_INPUT_TAGS, binaryTag,
                                    formatTag);
                            stringPointable.set(storages[0].getByteArray(), 1, storages[0].getLength());
                            formatPointable.set(storages[1].getByteArray(), 1, storages[1].getLength());
                            if (HEX_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                hexParser.generateByteArrayFromHexString(stringPointable.getByteArray(),
                                        stringPointable.getCharStartOffset(), stringPointable.getUTF8Length());

                                aBinary.setValue(hexParser.getByteArray(), 0, hexParser.getLength());
                            } else if (BASE64_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                base64Parser.generatePureByteArrayFromBase64String(stringPointable.getByteArray(),
                                        stringPointable.getCharStartOffset(), stringPointable.getUTF8Length());

                                aBinary.setValue(base64Parser.getByteArray(), 0, base64Parser.getLength());
                            } else {
                                throw new AlgebricksException(getIdentifier().getName()
                                        + ": expects format indicator of \"hex\" or \"base64\" in the 2nd argument");
                            }
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
