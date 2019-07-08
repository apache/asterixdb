
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

package org.apache.asterix.test.runtime;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.FunctionInfo;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionCollection;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * This test goes through all the functions that should respect the "missing/null in -> missing/null out" behavior
 * and ensures they respect that rule
 */

@RunWith(Parameterized.class)
public class NullMissingTest {

    private static final Logger LOGGER = LogManager.getLogger();

    // those are the functions that need IATypes of their args and use them in the function constructor
    private static Set<FunctionInfo> functionsRequiringTypes = new HashSet<>();

    @Parameters(name = "NullMissingTest {index}: {0}")
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

    @Parameter
    public String testName;

    @Parameter(1)
    public IFunctionDescriptor functionDescriptor;

    @Test
    public void test() throws Exception {
        String className = functionDescriptor.getClass().getName();
        LOGGER.log(Level.INFO, "Testing " + className);

        // Get the arguments combinations
        AbstractScalarFunctionDynamicDescriptor funcDesc = (AbstractScalarFunctionDynamicDescriptor) functionDescriptor;
        int inputArity = funcDesc.getIdentifier().getArity();
        Iterator<Pair<IScalarEvaluatorFactory[], IAType[]>> argEvalFactoryIterator = getArgCombinations(inputArity);
        int index = 0;

        // Test is happening here
        while (argEvalFactoryIterator.hasNext()) {
            Pair<IScalarEvaluatorFactory[], IAType[]> argumentsAndTypesPair = argEvalFactoryIterator.next();

            // Set the IAType if it's needed
            if (functionsRequiringTypes.contains(new FunctionInfo(funcDesc.getIdentifier(), true))) {
                funcDesc.setImmutableStates((Object[]) argumentsAndTypesPair.second);
            }

            // Evaluate
            IScalarEvaluatorFactory evalFactory = funcDesc.createEvaluatorFactory(argumentsAndTypesPair.first);
            IEvaluatorContext ctx = mock(IEvaluatorContext.class);
            IScalarEvaluator evaluator = evalFactory.createScalarEvaluator(ctx);
            IPointable resultPointable = new VoidPointable();
            evaluator.evaluate(null, resultPointable);

            // Result checks
            if (index != 0) {
                Assert.assertEquals(ATypeTag.SERIALIZED_MISSING_TYPE_TAG,
                        resultPointable.getByteArray()[resultPointable.getStartOffset()]);
            } else {
                Assert.assertEquals(ATypeTag.SERIALIZED_NULL_TYPE_TAG,
                        resultPointable.getByteArray()[resultPointable.getStartOffset()]);
            }
            ++index;
        }
    }

    // Generates combinations of arguments and their type
    private Iterator<Pair<IScalarEvaluatorFactory[], IAType[]>> getArgCombinations(int inputArity) {
        int argSize = inputArity >= 0 ? inputArity : 3;
        final int numCombinations = 1 << argSize;
        return new Iterator<Pair<IScalarEvaluatorFactory[], IAType[]>>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < numCombinations;
            }

            @Override
            public Pair<IScalarEvaluatorFactory[], IAType[]> next() {
                IAType[] argumentTypes = new IAType[argSize];
                IScalarEvaluatorFactory[] scalarEvaluatorFactories = new IScalarEvaluatorFactory[argSize];

                for (int j = 0; j < argSize; ++j) {
                    if ((index & (1 << j)) != 0) {
                        argumentTypes[j] = BuiltinType.AMISSING;
                        scalarEvaluatorFactories[j] =
                                new ConstantEvalFactory(new byte[] { ATypeTag.SERIALIZED_MISSING_TYPE_TAG });
                    } else {
                        argumentTypes[j] = BuiltinType.ANULL;
                        scalarEvaluatorFactories[j] =
                                new ConstantEvalFactory(new byte[] { ATypeTag.SERIALIZED_NULL_TYPE_TAG });
                    }
                }
                ++index;
                return new Pair<>(scalarEvaluatorFactories, argumentTypes);
            }
        };
    }

    // Functions that require the IAType for their test
    @BeforeClass
    public static void buildFunctionsRequiringType() {
        // Those are the functions that need IATypes of their args and use them in the function constructor
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.ARRAY_POSITION, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.ARRAY_CONTAINS, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.ARRAY_SORT, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.ARRAY_DISTINCT, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.EQ, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.LT, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.GT, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.GE, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.LE, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.NEQ, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.MISSING_IF, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.NAN_IF, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.NEGINF_IF, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.NULL_IF, true));
        functionsRequiringTypes.add(new FunctionInfo(BuiltinFunctions.POSINF_IF, true));
    }
}
