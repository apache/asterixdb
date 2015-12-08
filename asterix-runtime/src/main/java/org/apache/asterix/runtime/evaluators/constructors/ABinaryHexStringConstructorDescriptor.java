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

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
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
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.parsers.ByteArrayHexParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParser;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class ABinaryHexStringConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ABinaryHexStringConstructorDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ABinaryConstructorEvaluator(output, args[0], ByteArrayHexParserFactory.INSTANCE);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.BINARY_HEX_CONSTRUCTOR;
    }

    static class ABinaryConstructorEvaluator implements ICopyEvaluator {
        private DataOutput out;
        private ArrayBackedValueStorage outInput;
        private ICopyEvaluator eval;
        private IValueParser byteArrayParser;
        private UTF8StringPointable utf8Ptr = new UTF8StringPointable();

        @SuppressWarnings("unchecked")
        private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ANULL);

        public ABinaryConstructorEvaluator(final IDataOutputProvider output, ICopyEvaluatorFactory copyEvaluatorFactory,
                IValueParserFactory valueParserFactory) throws AlgebricksException {
            out = output.getDataOutput();
            outInput = new ArrayBackedValueStorage();
            eval = copyEvaluatorFactory.createEvaluator(outInput);
            byteArrayParser = valueParserFactory.createValueParser();
        }

        @Override
        public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

            try {
                outInput.reset();
                eval.evaluate(tuple);
                byte[] binary = outInput.getByteArray();

                ATypeTag tt = ATypeTag.VALUE_TYPE_MAPPING[binary[0]];
                if (tt == ATypeTag.NULL) {
                    nullSerde.serialize(ANull.NULL, out);
                } else if (tt == ATypeTag.BINARY) {
                    out.write(outInput.getByteArray(), outInput.getStartOffset(), outInput.getLength());
                } else if (tt == ATypeTag.STRING) {
                    utf8Ptr.set(outInput.getByteArray(), 1, outInput.getLength() - 1);

                    char[] buffer = utf8Ptr.toString().toCharArray();
                    out.write(ATypeTag.BINARY.serialize());
                    byteArrayParser.parse(buffer, 0, buffer.length, out);
                } else {
                    throw new AlgebricksException("binary type of " + tt + "haven't implemented yet.");
                }
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
        }
    }

    ;
}
