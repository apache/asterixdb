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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractBinaryScalarEvaluator implements IScalarEvaluator {

    protected ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected DataOutput dataOutput = resultStorage.getDataOutput();
    protected IPointable[] pointables;
    protected IScalarEvaluator[] evaluators;
    // Function ID, for error reporting.
    protected final FunctionIdentifier funcId;
    protected final SourceLocation sourceLoc;

    public AbstractBinaryScalarEvaluator(IEvaluatorContext context, final IScalarEvaluatorFactory[] evaluatorFactories,
            FunctionIdentifier funcId, SourceLocation sourceLoc) throws HyracksDataException {
        pointables = new IPointable[evaluatorFactories.length];
        evaluators = new IScalarEvaluator[evaluatorFactories.length];
        for (int i = 0; i < evaluators.length; ++i) {
            pointables[i] = new VoidPointable();
            evaluators[i] = evaluatorFactories[i].createScalarEvaluator(context);
        }
        this.funcId = funcId;
        this.sourceLoc = sourceLoc;
    }

    protected void checkTypeMachingThrowsIfNot(ATypeTag[] expected, ATypeTag... actual) throws HyracksDataException {
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual[i]) {
                if (!ATypeHierarchy.canPromote(actual[i], expected[i])
                        && !ATypeHierarchy.canPromote(expected[i], actual[i])) {
                    throw new TypeMismatchException(sourceLoc, funcId, i, actual[i].serialize(),
                            expected[i].serialize());
                }
            }
        }
    }

}
