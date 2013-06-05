/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.IOException;

import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.SimilarityJaccardPrefixEvaluator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class SimilarityJaccardPrefixCheckDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SimilarityJaccardPrefixCheckDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new SimilarityJaccardPrefixCheckEvaluator(args, output);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SIMILARITY_JACCARD_PREFIX_CHECK;
    }

    private static class SimilarityJaccardPrefixCheckEvaluator extends SimilarityJaccardPrefixEvaluator {

        private final OrderedListBuilder listBuilder;
        private ArrayBackedValueStorage inputVal;
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ABOOLEAN);
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<AFloat> floatSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.AFLOAT);
        private final AMutableFloat aFloat = new AMutableFloat(0);

        private final AOrderedListType listType = new AOrderedListType(BuiltinType.ANY, "list");

        public SimilarityJaccardPrefixCheckEvaluator(ICopyEvaluatorFactory[] args, IDataOutputProvider output)
                throws AlgebricksException {
            super(args, output);
            listBuilder = new OrderedListBuilder();
            inputVal = new ArrayBackedValueStorage();
        }

        @Override
        public void writeResult() throws AlgebricksException, IOException {
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
