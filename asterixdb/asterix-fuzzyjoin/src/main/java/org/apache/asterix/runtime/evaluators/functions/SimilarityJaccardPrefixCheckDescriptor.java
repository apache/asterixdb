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
package org.apache.asterix.runtime.evaluators.functions;

import java.io.IOException;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.SimilarityJaccardPrefixEvaluator;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class SimilarityJaccardPrefixCheckDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SimilarityJaccardPrefixCheckDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new SimilarityJaccardPrefixCheckEvaluator(args, ctx);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.SIMILARITY_JACCARD_PREFIX_CHECK;
    }

    private static class SimilarityJaccardPrefixCheckEvaluator extends SimilarityJaccardPrefixEvaluator {

        private final OrderedListBuilder listBuilder;
        private ArrayBackedValueStorage inputVal;
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ABoolean> booleanSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<AFloat> floatSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AFLOAT);
        private final AMutableFloat aFloat = new AMutableFloat(0);

        private final AOrderedListType listType = new AOrderedListType(BuiltinType.ANY, "list");

        public SimilarityJaccardPrefixCheckEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context)
                throws HyracksDataException {
            super(args, context);
            listBuilder = new OrderedListBuilder();
            inputVal = new ArrayBackedValueStorage();
        }

        @Override
        public void writeResult() throws HyracksDataException, IOException {
            listBuilder.reset(listType);
            boolean matches = (sim <= 0) ? false : true;
            float jaccSim = (matches) ? sim : 0.0f;

            inputVal.reset();
            booleanSerde.serialize(matches ? ABoolean.TRUE : ABoolean.FALSE, inputVal.getDataOutput());
            listBuilder.addItem(inputVal);

            inputVal.reset();
            aFloat.setValue(jaccSim);
            floatSerde.serialize(aFloat, inputVal.getDataOutput());
            listBuilder.addItem(inputVal);
            listBuilder.write(out, true);
        }
    }

}
