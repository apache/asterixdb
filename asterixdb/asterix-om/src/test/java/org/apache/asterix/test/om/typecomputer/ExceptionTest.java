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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.om.functions.BuiltinFunctions;
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
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
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
 * n = number of types available
 * m = number of arguments to pass
 *
 * Example:
 * 2 arguments, 40 types = 40 ^ 2 = 1,600 combinations per type computer
 * 3 arguments, 40 types = 40 ^ 3 = 64,000 combinations per type computer
 * 4 arguments, 40 types = 40 ^ 4 = 2,560,000 combinations per type computer
 *
 * Choice of number of arguments:
 * 1- To know the proper number of arguments for each type computer, we are going over all the available functions,
 *    getting their arity and their type computer and comparing them as follows:
 *     - If a function has VARARGS, that type computer is marked as VARARGS.
 *     - If multiple functions use the same type computer, the highest arity of all functions is used for that computer.
 * 2- In case of VARARGS or type computers that handle more than 3 arguments, we will use only 3 arguments, for the
 *    following reasons:
 *     - Going over 3 arguments will result in over 2,500,000 test cases, which will time out and fail the test.
 *     - (As of now) all the type computers using VARARGS need a minimum of 3 arguments or less to function properly,
 *       so passing 3 arguments will suffice to include all cases.
 *     - (As of now) all the type computers that handle more than 3 arguments return a constant type, nothing to test.
 */

@RunWith(Parameterized.class)
public class ExceptionTest {

    private static final Logger LOGGER = LogManager.getLogger();

    // Caution: Using more than 3 arguments will generate millions of combinations and the test will timeout and fail
    private static final int MAXIMUM_ARGUMENTS_ALLOWED = 3;

    // This map will hold each used type computer and its highest arity, which represents arguments number to pass
    private static Map<String, Integer> typeComputerToArgsCountMap = new HashMap<>();

    // Test parameters
    @Parameter
    public String testName;

    @Parameter(1)
    public Class<? extends IResultTypeComputer> clazz;

    @Parameters(name = "TypeComputerTest {index}: {0}")
    public static Collection<Object[]> tests() {

        // Prepare tests
        List<Object[]> tests = new ArrayList<>();

        // First, we will go over all the functions available, get their arity and type computers to know the number
        // of arguments to pass for each type computer
        Map<IFunctionInfo, IResultTypeComputer> funTypeComputerMap;
        try {
            Field field = BuiltinFunctions.class.getDeclaredField("funTypeComputer");
            field.setAccessible(true);
            funTypeComputerMap = (HashMap<IFunctionInfo, IResultTypeComputer>) field.get(null);
        } catch (Exception ex) {
            // this should never fail
            throw new IllegalStateException();
        }

        if (funTypeComputerMap != null) {
            // Note: If a type computer handles both known and VARARGS, VARARGS will be used
            for (HashMap.Entry<IFunctionInfo, IResultTypeComputer> entry : funTypeComputerMap.entrySet()) {
                // Some functions have a null type computer for some reason, ignore them
                if (entry.getValue() == null) {
                    continue;
                }

                // Type computer does not exist, add it with its arity to the map
                if (typeComputerToArgsCountMap.get(entry.getValue().getClass().getSimpleName()) == null) {
                    typeComputerToArgsCountMap.put(entry.getValue().getClass().getSimpleName(),
                            entry.getKey().getFunctionIdentifier().getArity());
                    continue;
                }

                // VARARGS functions, these are kept as is, put/update, no need for any comparison
                if (entry.getKey().getFunctionIdentifier().getArity() == FunctionIdentifier.VARARGS) {
                    typeComputerToArgsCountMap.put(entry.getValue().getClass().getSimpleName(),
                            entry.getKey().getFunctionIdentifier().getArity());
                    continue;
                }

                // We have it already, and it is of type VARARGS, we are not going to change it
                if (typeComputerToArgsCountMap
                        .get(entry.getValue().getClass().getSimpleName()) == FunctionIdentifier.VARARGS) {
                    continue;
                }

                // We have it already, if it has larger arity than our existing one, we will update it
                if (typeComputerToArgsCountMap.get(entry.getValue().getClass().getSimpleName()) < entry.getKey()
                        .getFunctionIdentifier().getArity()) {
                    typeComputerToArgsCountMap.put(entry.getValue().getClass().getSimpleName(),
                            entry.getKey().getFunctionIdentifier().getArity());
                }
            }
        }

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
        // If a type computer is not found in the map, then it is not used by any function
        if (typeComputerToArgsCountMap.get(clazz.getSimpleName()) == null) {
            LOGGER.log(Level.INFO, "TypeComputer " + clazz.getSimpleName() + " is not used by any functions");
            return;
        }

        // Refer to documentation on the top for the details of choosing the arguments count
        // Arguments list
        int typeComputerMaxArgs = typeComputerToArgsCountMap.get(clazz.getSimpleName());

        // If type computer takes zero arguments, there is nothing to do
        if (typeComputerMaxArgs == 0) {
            return;
        }

        // Calculate number of arguments and number of combinations
        int numberOfArguments =
                (typeComputerMaxArgs > MAXIMUM_ARGUMENTS_ALLOWED || typeComputerMaxArgs == FunctionIdentifier.VARARGS)
                        ? MAXIMUM_ARGUMENTS_ALLOWED : typeComputerMaxArgs;
        int numberOfCombinations = (int) Math.pow(ATypeTag.values().length, numberOfArguments);

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
