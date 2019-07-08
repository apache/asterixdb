
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

package org.apache.asterix.test.runtime;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionCollection;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This test goes through all the functions that should respect the "missing/null in -> missing/null out" behavior,
 * passes all possible combinations of ATypeType arguments and ensures the thrown Exceptions hae the appropriate error
 * codes.
 */

@RunWith(Parameterized.class)
public class NullMissingExceptionTest {

    private static final Logger LOGGER = LogManager.getLogger();

    @Parameterized.Parameters(name = "NullMissingTest {index}: {0}")
    public static Collection<Object[]> tests() {
        List<Object[]> tests = new ArrayList<>();

        // Get all the functions
        List<IFunctionDescriptorFactory> functions =
                FunctionCollection.createDefaultFunctionCollection().getFunctionDescriptorFactories();

        // Build the tests
        for (IFunctionDescriptorFactory functionFactory : functions) {
            String className = functionFactory.getClass().getName();
            IFunctionDescriptor functionDescriptor = functionFactory.createFunctionDescriptor();

            // Include only functions annotated with MissingNullInOutFunction
            if (functionDescriptor.getClass().isAnnotationPresent(MissingNullInOutFunction.class)) {

                // We test all functions except record and cast functions, which requires type settings (we test them
                // in runtime tests).
                if (!className.contains("record") && !className.contains("Cast")) {
                    tests.add(new Object[] { getTestName(functionDescriptor.getClass()), functionDescriptor });
                } else {
                    LOGGER.log(Level.INFO, "Excluding " + className);
                }
            } else {
                LOGGER.log(Level.INFO, "Excluding " + className);
            }
        }
        return tests;
    }

    private static String getTestName(Class<?> clazz) {
        if (clazz.getEnclosingClass() != null) {
            return clazz.getEnclosingClass().getSimpleName();
        } else if (clazz.getSimpleName().contains("$$Lambda")) {
            return clazz.getSimpleName().replaceAll("\\$\\$Lambda.*", "");
        } else {
            return clazz.getSimpleName();
        }
    }

    @Parameterized.Parameter
    public String testName;

    @Parameterized.Parameter(1)
    public IFunctionDescriptor functionDescriptor;

    @Test
    public void test() throws Exception {
        String className = functionDescriptor.getClass().getName();
        LOGGER.log(Level.INFO, "Testing " + className);

        // Get the arguments combinations
        AbstractScalarFunctionDynamicDescriptor funcDesc = (AbstractScalarFunctionDynamicDescriptor) functionDescriptor;
        int inputArity = funcDesc.getIdentifier().getArity();
        Iterator<IScalarEvaluatorFactory[]> argEvalFactoryIterator = getArgCombinations(inputArity);

        // Test is happening here
        while (argEvalFactoryIterator.hasNext()) {
            IScalarEvaluatorFactory[] factories = argEvalFactoryIterator.next();

            // Evaluate
            IScalarEvaluatorFactory evalFactory = funcDesc.createEvaluatorFactory(factories);
            IEvaluatorContext ctx = mock(IEvaluatorContext.class);
            try {
                IScalarEvaluator evaluator = evalFactory.createScalarEvaluator(ctx);
                IPointable resultPointable = new VoidPointable();
                evaluator.evaluate(null, resultPointable);
            } catch (Throwable e) {
                String msg = e.getMessage();
                if (msg == null) {
                    continue;
                }
                if (msg.startsWith("ASX")) {
                    // Verifies the error code.
                    int errorCode = Integer.parseInt(msg.substring(3, 7));
                    Assert.assertTrue(errorCode >= 0 && errorCode < 1000);
                } else if (msg.startsWith("HYR")) {
                    // Verifies the error code.
                    int errorCode = Integer.parseInt(msg.substring(3, 7));
                    Assert.assertTrue(errorCode >= 0 && errorCode < 1000);
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
