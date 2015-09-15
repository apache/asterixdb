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
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
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
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class FindBinaryDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override public IFunctionDescriptor createFunctionDescriptor() {
            return new FindBinaryDescriptor();
        }
    };

    @Override public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.FIND_BINARY;
    }

    private static final ATypeTag[] EXPECTED_INPUT_TAG = { ATypeTag.BINARY, ATypeTag.BINARY };

    @Override public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            @Override public ICopyEvaluator createEvaluator(final IDataOutputProvider output)
                    throws AlgebricksException {
                return new AbstractFindBinaryCopyEvaluator(output, args, getIdentifier().getName()) {
                    @Override protected int getFromOffset(IFrameTupleReference tuple) throws AlgebricksException {
                        return 0;
                    }
                };
            }
        };
    }

    static abstract class AbstractFindBinaryCopyEvaluator extends AbstractCopyEvaluator {

        public AbstractFindBinaryCopyEvaluator(IDataOutputProvider output,
                ICopyEvaluatorFactory[] copyEvaluatorFactories, String functionName) throws AlgebricksException {
            super(output, copyEvaluatorFactories);
            this.functionName = functionName;
        }

        protected String functionName;
        protected AMutableInt64 result = new AMutableInt64(-1);

        @SuppressWarnings("unchecked")
        protected ISerializerDeserializer<AInt64> intSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.AINT64);

        @Override public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
            ATypeTag textTag = evaluateTuple(tuple, 0);
            ATypeTag wordTag = evaluateTuple(tuple, 1);
            int fromOffset = getFromOffset(tuple);

            try {
                if (serializeNullIfAnyNull(textTag, wordTag)) {
                    return;
                }
                checkTypeMachingThrowsIfNot(functionName, EXPECTED_INPUT_TAG, textTag,
                        wordTag);

                byte[] textBytes = storages[0].getByteArray();
                byte[] wordBytes = storages[1].getByteArray();
                int textLength = ByteArrayPointable.getLength(textBytes, 1);
                int wordLength = ByteArrayPointable.getLength(wordBytes, 1);
                result.setValue(1 + indexOf(textBytes, 3, textLength, wordBytes, 3, wordLength, fromOffset));
                intSerde.serialize(result, dataOutput);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
        }

        protected abstract int getFromOffset(IFrameTupleReference tuple) throws AlgebricksException;
    }

    // copy from String.indexOf(String)
    static int indexOf(byte[] source, int sourceOffset, int sourceCount,
            byte[] target, int targetOffset, int targetCount,
            int fromIndex) {
        if (fromIndex >= sourceCount) {
            return (targetCount == 0 ? sourceCount : -1);
        }
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        if (targetCount == 0) {
            return fromIndex;
        }

        byte first = target[targetOffset];
        int max = sourceOffset + (sourceCount - targetCount);

        for (int i = sourceOffset + fromIndex; i <= max; i++) {
            /* Look for first character. */
            if (source[i] != first) {
                while (++i <= max && source[i] != first)
                    ;
            }

            /* Found first character, now look at the rest of v2 */
            if (i <= max) {
                int j = i + 1;
                int end = j + targetCount - 1;
                for (int k = targetOffset + 1; j < end && source[j]
                        == target[k]; j++, k++)
                    ;

                if (j == end) {
                    /* Found whole string. */
                    return i - sourceOffset;
                }
            }
        }
        return -1;
    }

}
