
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.asterix.runtime;

import static org.mockito.Mockito.mock;

import java.util.Iterator;
import java.util.List;

import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.translator.util.FunctionCollection;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.junit.Assert;
import org.junit.Test;

public class ExceptionTest {

    @Test
    public void test() throws Exception {
        List<IFunctionDescriptorFactory> functions = FunctionCollection.getFunctionDescriptorFactories();
        int testedFunctions = 0;
        for (IFunctionDescriptorFactory func : functions) {
            String className = func.getClass().getName();
            // We test all generated functions except
            // record and cast functions, which requires type settings.
            if (className.contains("Gen") && !className.contains("record") && !className.contains("Cast")) {
                testFunction(func);
                ++testedFunctions;
            }
        }
        // 208 is the current number of functions with generated code.
        Assert.assertTrue(testedFunctions >= 208);
    }

    private void testFunction(IFunctionDescriptorFactory funcFactory) throws Exception {
        AbstractScalarFunctionDynamicDescriptor funcDesc = (AbstractScalarFunctionDynamicDescriptor) funcFactory
                .createFunctionDescriptor();
        int inputArity = funcDesc.getIdentifier().getArity();
        Iterator<IScalarEvaluatorFactory[]> argEvalFactoryIterator = getArgCombinations(inputArity);
        while (argEvalFactoryIterator.hasNext()) {
            IScalarEvaluatorFactory evalFactory = funcDesc.createEvaluatorFactory(argEvalFactoryIterator.next());
            IHyracksTaskContext ctx = mock(IHyracksTaskContext.class);
            IScalarEvaluator evaluator = evalFactory.createScalarEvaluator(ctx);
            IPointable resultPointable = new VoidPointable();
            try {
                evaluator.evaluate(null, resultPointable);
            } catch (Throwable e) {
                if (e.getMessage() == null) {
                    continue;
                }
                if (e.getMessage().startsWith("ASX")) {
                    continue;
                } else {
                    // Any root-level data exceptions thrown from runtime functions should have an error code.
                    Assert.assertTrue(!(e instanceof HyracksDataException) || (e.getCause() != null));
                }
            }
        }
    }

    private Iterator<IScalarEvaluatorFactory[]> getArgCombinations(final int inputArity) {
        final int argSize = inputArity >= 0 ? inputArity : 3;
        final int numCombinations = (int) Math.pow(ATypeTag.values().length, argSize);
        return new Iterator<IScalarEvaluatorFactory[]>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < numCombinations;
            }

            @Override
            public IScalarEvaluatorFactory[] next() {
                IScalarEvaluatorFactory[] scalarEvaluatorFactories = new IScalarEvaluatorFactory[argSize];
                for (int j = 0; j < argSize; ++j) {
                    int base = (int) Math.pow(ATypeTag.values().length, j);
                    // Enumerates through all possible type tags.
                    byte serializedTypeTag = (byte) ((index / base) % ATypeTag.values().length);
                    scalarEvaluatorFactories[j] = new ConstantEvalFactory(new byte[] { serializedTypeTag });
                }
                ++index;
                return scalarEvaluatorFactories;
            }

        };

    }

}
