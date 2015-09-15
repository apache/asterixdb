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

import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Checks whether a list with an edit distance threshold can be filtered with a lower bounding on the number of common list elements.
 * This function returns 'true' if the lower bound on the number of common elements is positive, 'false' otherwise.
 * For example, this function is used during an indexed nested-loop join based on edit distance. We partition the tuples of the probing
 * dataset into those that are filterable and those that are not. Those that are filterable are forwarded to the index. The others are
 * are fed into a (non indexed) nested-loop join.
 */
public class EditDistanceListIsFilterable extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new EditDistanceListIsFilterable();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
                return new EditDistanceListIsFilterableEvaluator(args, output);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.EDIT_DISTANCE_LIST_IS_FILTERABLE;
    }

    private static class EditDistanceListIsFilterableEvaluator implements ICopyEvaluator {

        protected final ArrayBackedValueStorage argBuf = new ArrayBackedValueStorage();
        protected final IDataOutputProvider output;

        protected final ICopyEvaluator listEval;
        protected final ICopyEvaluator edThreshEval;

        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ABOOLEAN);

        public EditDistanceListIsFilterableEvaluator(ICopyEvaluatorFactory[] args, IDataOutputProvider output)
                throws AlgebricksException {
            this.output = output;
            listEval = args[0].createEvaluator(argBuf);
            edThreshEval = args[1].createEvaluator(argBuf);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
            ATypeTag typeTag = null;

            // Check type and compute string length.
            argBuf.reset();
            listEval.evaluate(tuple);
            typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argBuf.getByteArray()[0]);
            long listLen = 0;
            switch (typeTag) {
                case UNORDEREDLIST: {
                    listLen = AUnorderedListSerializerDeserializer.getNumberOfItems(argBuf.getByteArray(), 0);
                    break;
                }
                case ORDEREDLIST: {
                    listLen = AOrderedListSerializerDeserializer.getNumberOfItems(argBuf.getByteArray(), 0);
                    break;
                }
                default: {
                    throw new AlgebricksException(AsterixBuiltinFunctions.EDIT_DISTANCE_LIST_IS_FILTERABLE.getName()
                            + ": expects input type ORDEREDLIST or UNORDEREDLIST as the first argument, but got "
                            + typeTag + ".");
                }
            }

            // Check type and extract edit-distance threshold.
            argBuf.reset();
            edThreshEval.evaluate(tuple);
            typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argBuf.getByteArray()[0]);
            long edThresh;

            try {
                edThresh = ATypeHierarchy.getIntegerValue(argBuf.getByteArray(), 0);
            } catch (HyracksDataException e1) {
                throw new AlgebricksException(e1);
            }

            // Compute result.
            long lowerBound = listLen - edThresh;
            try {
                if (lowerBound <= 0) {
                    booleanSerde.serialize(ABoolean.FALSE, output.getDataOutput());
                } else {
                    booleanSerde.serialize(ABoolean.TRUE, output.getDataOutput());
                }
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
        }
    }
}
