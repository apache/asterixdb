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

package org.apache.asterix.test.om.typecomputer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

/**
 * This test passes all possible combinations of types to each type computer, to ensure each type computer handles
 * them properly, and in case of an exception, the exception falls within the defined exception codes.
 *
 * Care needs to be taken when deciding the number of arguments, below is how the tests are calculated:
 * The number of combinations is = n ^ m
 * n = number of arguments
 * m = number of types available
 *
 * Example:
 * 2 arguments, 40 types = 1600 combinations per type computer
 * 3 arguments, 40 types = 64000 combinations per type computer
 *
 * TODO There are 2 functions that expect a minimum arguments of 3, so passing them 2 arguments will cause them
 * to return an exception, so they're really not being tested properly now. Probably need to add an exception list
 * for such functions.
 * Ideally, making the arguments in the test to be 3 will solve the issue, but that would push the tests to 64000 each
 * and that takes too long and times out the test apparently.
 */

@RunWith(Parameterized.class)
public class ExceptionTest {

    private static final Logger LOGGER = LogManager.getLogger();

    // Number of arguments to be passed. Number of combinations depends on number of arguments
    private static int numberOfArguments = 2;
    private static int numberOfCombinations = (int) Math.pow(ATypeTag.values().length, numberOfArguments);

    // Test parameters
    @Parameter
    public String testName;

    @Parameter(1)
    public Class<? extends IResultTypeComputer> clazz;

    @Parameters(name = "TypeComputerTest {index}: {0}")
    public static Collection<Object[]> tests() {

        // Prepare tests
        List<Object[]> tests = new ArrayList<>();

        // Tests all usual type computers.
        Reflections reflections = new Reflections("org.apache.asterix.om.typecomputer", new SubTypesScanner(false));
        Set<Class<? extends IResultTypeComputer>> classes = reflections.getSubTypesOf(IResultTypeComputer.class);

        for (Class<? extends IResultTypeComputer> clazz : classes) {
            if (Modifier.isAbstract(clazz.getModifiers())) {
                LOGGER.log(Level.INFO, "Excluding " + clazz.getSimpleName() + " (Abstract class)");
                continue;
            }

            tests.add(new Object[] { clazz.getSimpleName(), clazz });
        }

        return tests;
    }

    @Test
    public void test() throws Exception {

        // Arguments list
        Object[] opaqueParameters = new Object[numberOfArguments];
        List<Mutable<ILogicalExpression>> argumentsList = new ArrayList<>(numberOfArguments);
        for (int i = 0; i < numberOfArguments; i++) {
            opaqueParameters[i] = BuiltinType.ANY;
        }

        // Sets arguments for the mocked function expression.
        for (int index = 0; index < numberOfCombinations; ++index) {

            argumentsList.clear();

            // Type environment and metadata provider
            IVariableTypeEnvironment typeEnv = mock(IVariableTypeEnvironment.class);
            IMetadataProvider metadataProvider = mock(IMetadataProvider.class);

            // Function identifier
            FunctionIdentifier functionIdentifier = mock(FunctionIdentifier.class);
            when(functionIdentifier.getName()).thenReturn("testFunction");

            // Function call expression
            AbstractFunctionCallExpression functionCallExpression = mock(AbstractFunctionCallExpression.class);
            when(functionCallExpression.getFunctionIdentifier()).thenReturn(functionIdentifier);

            try {
                for (int argIndex = 0; argIndex < numberOfArguments; ++argIndex) {
                    int base = (int) Math.pow(ATypeTag.values().length, argIndex);
                    int serializedTypeTag = (index / base) % ATypeTag.values().length + 1;

                    ATypeTag typeTag = ATypeTag.VALUE_TYPE_MAPPING[serializedTypeTag];
                    if (typeTag == null) {
                        // If any tags have been removed or are missing, replace them with any
                        typeTag = ATypeTag.ANY;
                    }

                    // Set the types
                    IAType iaType = mock(IAType.class);
                    when(iaType.getTypeTag()).thenReturn(typeTag);

                    ILogicalExpression argument = mock(ILogicalExpression.class);
                    when(typeEnv.getType(argument)).thenReturn(iaType);

                    // Add argument to the arguments list
                    argumentsList.add(new MutableObject<>(argument));
                }

                // Sets up arguments for the mocked expression.
                when(functionCallExpression.getArguments()).thenReturn(argumentsList);
                when(functionCallExpression.getOpaqueParameters()).thenReturn(opaqueParameters);

                // Invokes a type computer.
                IResultTypeComputer instance;
                Field[] fields = clazz.getFields();

                for (Field field : fields) {
                    if (field.getType().equals(clazz)) {
                        instance = (IResultTypeComputer) field.get(null);
                        instance.computeType(functionCallExpression, typeEnv, metadataProvider);
                    }
                }
            } catch (AlgebricksException ae) {
                String msg = ae.getMessage();
                if (msg.startsWith("ASX")) {
                    // Verifies the error code.
                    int errorCode = Integer.parseInt(msg.substring(3, 7));
                    Assert.assertTrue(errorCode >= 1000 && errorCode < 2000);
                } else {
                    // Any root-level compilation exceptions thrown from type computers should have an error code.
                    Assert.assertTrue((ae.getCause() != null));
                }
            } catch (ClassCastException e) {
                // Do nothing
            }
        }
    }
}
