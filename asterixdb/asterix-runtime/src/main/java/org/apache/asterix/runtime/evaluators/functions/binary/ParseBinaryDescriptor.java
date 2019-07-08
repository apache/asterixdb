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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.AMutableBinary;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.bytes.Base64Parser;
import org.apache.hyracks.util.bytes.HexParser;

@MissingNullInOutFunction
public class ParseBinaryDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private static final UTF8StringPointable HEX_FORMAT = UTF8StringPointable.generateUTF8Pointable("hex");
    private static final UTF8StringPointable BASE64_FORMAT = UTF8StringPointable.generateUTF8Pointable("base64");

    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ParseBinaryDescriptor();
        }
    };

    public static final ATypeTag[] EXPECTED_INPUT_TAGS = { ATypeTag.STRING, ATypeTag.STRING };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.PARSE_BINARY;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractBinaryScalarEvaluator(ctx, args, getIdentifier(), sourceLoc) {

                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ABinary> binarySerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABINARY);

                    private AMutableBinary aBinary = new AMutableBinary(new byte[0], 0, 0);
                    private final UTF8StringPointable stringPointable = new UTF8StringPointable();
                    private final UTF8StringPointable formatPointable = new UTF8StringPointable();

                    private final HexParser hexParser = new HexParser();
                    private final Base64Parser base64Parser = new Base64Parser();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        evaluators[0].evaluate(tuple, pointables[0]);
                        evaluators[1].evaluate(tuple, pointables[1]);

                        if (PointableHelper.checkAndSetMissingOrNull(result, pointables[0], pointables[1])) {
                            return;
                        }

                        ATypeTag binaryTag = ATypeTag.VALUE_TYPE_MAPPING[pointables[0].getByteArray()[pointables[0]
                                .getStartOffset()]];

                        ATypeTag formatTag = ATypeTag.VALUE_TYPE_MAPPING[pointables[1].getByteArray()[pointables[1]
                                .getStartOffset()]];
                        checkTypeMachingThrowsIfNot(EXPECTED_INPUT_TAGS, binaryTag, formatTag);
                        stringPointable.set(pointables[0].getByteArray(), pointables[0].getStartOffset() + 1,
                                pointables[0].getLength());
                        formatPointable.set(pointables[1].getByteArray(), pointables[1].getStartOffset() + 1,
                                pointables[1].getLength());
                        if (HEX_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                            hexParser.generateByteArrayFromHexString(stringPointable.getByteArray(),
                                    stringPointable.getCharStartOffset(), stringPointable.getUTF8Length());

                            aBinary.setValue(hexParser.getByteArray(), 0, hexParser.getLength());
                        } else if (BASE64_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                            base64Parser.generatePureByteArrayFromBase64String(stringPointable.getByteArray(),
                                    stringPointable.getCharStartOffset(), stringPointable.getUTF8Length());

                            aBinary.setValue(base64Parser.getByteArray(), 0, base64Parser.getLength());
                        } else {
                            throw new UnsupportedItemTypeException(sourceLoc, getIdentifier(), formatTag.serialize());
                        }
                        binarySerde.serialize(aBinary, dataOutput);
                        result.set(resultStorage);
                    }
                };

            }

        };
    }

}
