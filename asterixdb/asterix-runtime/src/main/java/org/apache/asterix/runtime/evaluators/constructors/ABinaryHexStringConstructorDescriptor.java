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

package org.apache.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.parsers.ByteArrayHexParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;

@MissingNullInOutFunction
public class ABinaryHexStringConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ABinaryHexStringConstructorDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new ABinaryConstructorEvaluator(args[0], ByteArrayHexParserFactory.INSTANCE, ctx, sourceLoc);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.BINARY_HEX_CONSTRUCTOR;
    }

    static class ABinaryConstructorEvaluator implements IScalarEvaluator {
        private final SourceLocation sourceLoc;
        private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
        private final DataOutput out = resultStorage.getDataOutput();
        private final IPointable inputArg = new VoidPointable();
        private final IScalarEvaluator eval;
        private IValueParser byteArrayParser;
        private UTF8StringPointable utf8Ptr = new UTF8StringPointable();

        public ABinaryConstructorEvaluator(IScalarEvaluatorFactory copyEvaluatorFactory,
                IValueParserFactory valueParserFactory, IEvaluatorContext context, SourceLocation sourceLoc)
                throws HyracksDataException {
            this.sourceLoc = sourceLoc;
            eval = copyEvaluatorFactory.createScalarEvaluator(context);
            byteArrayParser = valueParserFactory.createValueParser();
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            try {
                eval.evaluate(tuple, inputArg);

                if (PointableHelper.checkAndSetMissingOrNull(result, inputArg)) {
                    return;
                }

                byte[] binary = inputArg.getByteArray();
                int startOffset = inputArg.getStartOffset();
                int len = inputArg.getLength();

                byte tt = binary[startOffset];
                if (tt == ATypeTag.SERIALIZED_BINARY_TYPE_TAG) {
                    result.set(inputArg);
                } else if (tt == ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                    resultStorage.reset();
                    utf8Ptr.set(inputArg.getByteArray(), startOffset + 1, len - 1);
                    char[] buffer = utf8Ptr.toString().toCharArray();
                    out.write(ATypeTag.BINARY.serialize());
                    byteArrayParser.parse(buffer, 0, buffer.length, out);
                    result.set(resultStorage);
                } else {
                    throw new TypeMismatchException(sourceLoc, BuiltinFunctions.BINARY_HEX_CONSTRUCTOR, 0, tt,
                            ATypeTag.SERIALIZED_BINARY_TYPE_TAG, ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                }
            } catch (IOException e) {
                throw new InvalidDataFormatException(sourceLoc, BuiltinFunctions.BINARY_HEX_CONSTRUCTOR, e,
                        ATypeTag.SERIALIZED_BINARY_TYPE_TAG);
            }
        }
    }

}
