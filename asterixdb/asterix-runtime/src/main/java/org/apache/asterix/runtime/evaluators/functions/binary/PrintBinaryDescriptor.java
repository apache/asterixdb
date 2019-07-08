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

import java.io.IOException;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.bytes.Base64Printer;
import org.apache.hyracks.util.bytes.HexPrinter;
import org.apache.hyracks.util.string.UTF8StringWriter;

@MissingNullInOutFunction
public class PrintBinaryDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private static final UTF8StringPointable HEX_FORMAT = UTF8StringPointable.generateUTF8Pointable("hex");
    private static final UTF8StringPointable BASE64_FORMAT = UTF8StringPointable.generateUTF8Pointable("base64");

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.PRINT_BINARY;
    }

    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new PrintBinaryDescriptor();
        }
    };

    public final static ATypeTag[] EXPECTED_INPUT_TAGS = { ATypeTag.BINARY, ATypeTag.STRING };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractBinaryScalarEvaluator(ctx, args, getIdentifier(), sourceLoc) {

                    private StringBuilder stringBuilder = new StringBuilder();
                    private final ByteArrayPointable byteArrayPtr = new ByteArrayPointable();
                    private final UTF8StringPointable formatPointable = new UTF8StringPointable();
                    private final UTF8StringWriter writer = new UTF8StringWriter();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        evaluators[0].evaluate(tuple, pointables[0]);
                        evaluators[1].evaluate(tuple, pointables[1]);

                        if (PointableHelper.checkAndSetMissingOrNull(result, pointables[0], pointables[1])) {
                            return;
                        }

                        try {
                            ATypeTag arg0Tag = ATypeTag.VALUE_TYPE_MAPPING[pointables[0].getByteArray()[pointables[0]
                                    .getStartOffset()]];
                            ATypeTag arg1Tag = ATypeTag.VALUE_TYPE_MAPPING[pointables[1].getByteArray()[pointables[1]
                                    .getStartOffset()]];
                            checkTypeMachingThrowsIfNot(EXPECTED_INPUT_TAGS, arg0Tag, arg1Tag);

                            byteArrayPtr.set(pointables[0].getByteArray(), pointables[0].getStartOffset() + 1,
                                    pointables[0].getLength());
                            formatPointable.set(pointables[1].getByteArray(), pointables[1].getStartOffset() + 1,
                                    pointables[1].getLength());

                            int lengthBinary = byteArrayPtr.getContentLength();
                            stringBuilder.setLength(0);
                            if (HEX_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                HexPrinter.printHexString(byteArrayPtr.getByteArray(),
                                        byteArrayPtr.getContentStartOffset(), lengthBinary, stringBuilder);
                            } else if (BASE64_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                Base64Printer.printBase64Binary(byteArrayPtr.getByteArray(),
                                        byteArrayPtr.getContentStartOffset(), lengthBinary, stringBuilder);
                            } else {
                                throw new UnsupportedItemTypeException(sourceLoc, getIdentifier(), arg1Tag.serialize());
                            }
                            dataOutput.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            writer.writeUTF8(stringBuilder.toString(), dataOutput);
                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };

    }
}
