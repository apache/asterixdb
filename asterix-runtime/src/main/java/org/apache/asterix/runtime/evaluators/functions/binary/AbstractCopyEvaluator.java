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
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import java.io.DataOutput;

public abstract class AbstractCopyEvaluator implements ICopyEvaluator {
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);

    protected DataOutput dataOutput;
    protected ArrayBackedValueStorage[] storages;
    protected ICopyEvaluator[] evaluators;

    public AbstractCopyEvaluator(final IDataOutputProvider output, final ICopyEvaluatorFactory[] copyEvaluatorFactories)
            throws AlgebricksException {
        dataOutput = output.getDataOutput();
        storages = new ArrayBackedValueStorage[copyEvaluatorFactories.length];
        evaluators = new ICopyEvaluator[copyEvaluatorFactories.length];
        for (int i = 0; i < evaluators.length; ++i) {
            storages[i] = new ArrayBackedValueStorage();
            evaluators[i] = copyEvaluatorFactories[i].createEvaluator(storages[i]);
        }
    }

    public ATypeTag evaluateTuple(IFrameTupleReference tuple, int id) throws AlgebricksException {
        storages[id].reset();
        evaluators[id].evaluate(tuple);
        return ATypeTag.VALUE_TYPE_MAPPING[storages[id].getByteArray()[0]];
    }

    public boolean serializeNullIfAnyNull(ATypeTag... tags) throws HyracksDataException {
        for (ATypeTag typeTag : tags) {
            if (typeTag == ATypeTag.NULL) {
                nullSerde.serialize(ANull.NULL, dataOutput);
                return true;
            }
        }
        return false;
    }

    private static final String FIRST = "1st";
    private static final String SECOND = "2nd";
    private static final String THIRD = "3rd";
    private static final String TH = "th";

    public static String rankToString(int i) {
        String prefix = "";
        if (i >= 10) {
            prefix = String.valueOf(i / 10);
        }
        switch (i % 10) {
            case 1:
                return prefix + FIRST;
            case 2:
                return prefix + SECOND;
            case 3:
                return prefix + THIRD;
            default:
                return String.valueOf(i) + TH;
        }
    }

    public static void checkTypeMachingThrowsIfNot(String title, ATypeTag[] expected, ATypeTag... actual)
            throws AlgebricksException {
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual[i]) {
                if (!ATypeHierarchy.canPromote(actual[i], expected[i])
                        && !ATypeHierarchy.canPromote(expected[i], actual[i])) {
                    throw new AlgebricksException(title + ": expects " + expected[i] + " at " + rankToString(i + 1)
                            + " argument, but got " + actual[i]);
                }
            }
        }
    }

}
