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

import java.io.DataOutput;

import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractBinaryScalarEvaluator implements IScalarEvaluator {

    protected ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected DataOutput dataOutput = resultStorage.getDataOutput();
    protected IPointable[] pointables;
    protected IScalarEvaluator[] evaluators;

    public AbstractBinaryScalarEvaluator(final IHyracksTaskContext context,
            final IScalarEvaluatorFactory[] evaluatorFactories) throws HyracksDataException {
        pointables = new IPointable[evaluatorFactories.length];
        evaluators = new IScalarEvaluator[evaluatorFactories.length];
        for (int i = 0; i < evaluators.length; ++i) {
            pointables[i] = new VoidPointable();
            evaluators[i] = evaluatorFactories[i].createScalarEvaluator(context);
        }
    }

    private static final String FIRST = "1st";
    private static final String SECOND = "2nd";
    private static final String THIRD = "3rd";
    private static final String TH = "th";

    protected String rankToString(int i) {
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

    protected void checkTypeMachingThrowsIfNot(String title, ATypeTag[] expected, ATypeTag... actual)
            throws HyracksDataException {
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual[i]) {
                if (!ATypeHierarchy.canPromote(actual[i], expected[i])
                        && !ATypeHierarchy.canPromote(expected[i], actual[i])) {
                    throw new TypeMismatchException(title, i, actual[i].serialize(), expected[i].serialize());
                }
            }
        }
    }

}
