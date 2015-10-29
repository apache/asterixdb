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

import static org.apache.asterix.runtime.evaluators.functions.binary.ParseBinaryDescriptor.BASE64_FORMAT;
import static org.apache.asterix.runtime.evaluators.functions.binary.ParseBinaryDescriptor.HEX_FORMAT;

import java.io.IOException;

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.bytes.Base64Printer;
import org.apache.hyracks.util.bytes.HexPrinter;
import org.apache.hyracks.util.string.UTF8StringWriter;

public class PrintBinaryDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final byte SER_STRING_BYTE = ATypeTag.STRING.serialize();

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.PRINT_BINARY;
    }

    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new PrintBinaryDescriptor();
        }
    };

    public final static ATypeTag[] EXPECTED_INPUT_TAGS = { ATypeTag.BINARY, ATypeTag.STRING };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output)
                    throws AlgebricksException {
                return new AbstractCopyEvaluator(output, args) {

                    private StringBuilder stringBuilder = new StringBuilder();
                    private final ByteArrayPointable byteArrayPtr = new ByteArrayPointable();
                    private final UTF8StringPointable formatPointable = new UTF8StringPointable();
                    private final UTF8StringWriter writer = new UTF8StringWriter();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        ATypeTag arg0Tag = evaluateTuple(tuple, 0);
                        ATypeTag arg1Tag = evaluateTuple(tuple, 1);

                        try {
                            if (serializeNullIfAnyNull(arg0Tag, arg1Tag)) {
                                return;
                            }
                            checkTypeMachingThrowsIfNot(getIdentifier().getName(), EXPECTED_INPUT_TAGS, arg0Tag,
                                    arg1Tag);

                            byteArrayPtr.set(storages[0].getByteArray(), 1, storages[0].getLength());
                            formatPointable.set(storages[1].getByteArray(), 1, storages[1].getLength());

                            int lengthBinary = byteArrayPtr.getContentLength();
                            stringBuilder.setLength(0);
                            if (HEX_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                HexPrinter.printHexString(byteArrayPtr.getByteArray(),
                                        byteArrayPtr.getContentStartOffset(), lengthBinary, stringBuilder);
                            } else if (BASE64_FORMAT.ignoreCaseCompareTo(formatPointable) == 0) {
                                Base64Printer.printBase64Binary(byteArrayPtr.getByteArray(),
                                        byteArrayPtr.getContentStartOffset(), lengthBinary, stringBuilder);
                            } else {
                                throw new AlgebricksException(getIdentifier().getName()
                                        + ": expects format indicator of \"hex\" or \"base64\" in the 2nd argument");
                            }
                            dataOutput.writeByte(SER_STRING_BYTE);
                            writer.writeUTF8(stringBuilder.toString(), dataOutput);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }
                };
            }
        };

    }
}
