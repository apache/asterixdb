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

package org.apache.asterix.test.om.typecomputer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
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
 * This test passes a number of arguments to the type computers, all arguments of type "any", and ensures that
 * each computer handles the "any" type properly. The expected behavior for each type computer is to return "any",
 * "nullable" or "missable" type result.
 *
 * Things to note:
 * - The function passes 6 "any" arguments because, as of now, no function that we have has more than 6 arguments.
 * two other lists are made (3 args and 4 args), those are needed by specific type computers for now, those should be
 * changed and then the lists can be removed from the test.
 * - Some functions have a different behavior with "any" value, those will be added to an exception list.
 * - Some functions check their arguments count, this will make passing 6 arguments fail,
 * those are added to exception list.
 */

@RunWith(Parameterized.class)
public class TypeComputerTest {

    private static final Logger LOGGER = LogManager.getLogger();

    // type computers that have a different behavior when handling "any" type
    private static Set<String> differentBehaviorFunctions = new HashSet<>();

    // TODO(Hussain) Remove this after the type computers have been updated
    // type computers that check the number of arguments
    private static HashMap<Field, List<Mutable<ILogicalExpression>>> checkArgsCountFunctions = new HashMap<>();

    // Test parameters
    @Parameter
    public String testName;

    @Parameter(1)
    public Class<? extends IResultTypeComputer> clazz;

    @Parameters(name = "TypeComputerTest {index}: {0}")
    public static Collection<Object[]> tests() {

        // Do any needed preparations once
        prepare();

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

            if (differentBehaviorFunctions.contains(clazz.getSimpleName())) {
                LOGGER.log(Level.INFO, "Excluding " + clazz.getSimpleName() + " (Special behavior)");
                continue;
            }

            tests.add(new Object[] { clazz.getSimpleName(), clazz });
        }

        return tests;
    }

    /**
     * Test return types should be either "any", "missable" or "nullable"
     *
     * @throws Exception Exception
     */
    @Test
    public void test() throws Exception {

        // Type environment and metadata provider
        IVariableTypeEnvironment typeEnv = mock(IVariableTypeEnvironment.class);
        IMetadataProvider metadataProvider = mock(IMetadataProvider.class);

        // Arguments all of type "any"
        List<Mutable<ILogicalExpression>> threeArgs = createArgs(3, typeEnv);
        List<Mutable<ILogicalExpression>> fourArgs = createArgs(4, typeEnv);
        List<Mutable<ILogicalExpression>> sixArgs = createArgs(6, typeEnv);

        // TODO(Hussain) Remove this after the type computers are updated
        // Add to exception list for computers checking their arguments count
        addComputersCheckingArgsCount(threeArgs, fourArgs);

        // Mocks function identifier
        FunctionIdentifier functionIdentifier = mock(FunctionIdentifier.class);
        when(functionIdentifier.getName()).thenReturn("testFunction");

        // Mocks function expression.
        AbstractFunctionCallExpression functionCallExpression = mock(AbstractFunctionCallExpression.class);
        when(functionCallExpression.getFunctionIdentifier()).thenReturn(functionIdentifier);

        // Sets up required/actual types of the mocked expression.
        Object[] opaqueParameters = new Object[] { BuiltinType.ANY, BuiltinType.ANY };
        when(functionCallExpression.getOpaqueParameters()).thenReturn(opaqueParameters);

        // Tests the return type. It should be either ANY or NULLABLE/MISSABLE.
        IResultTypeComputer instance;
        IAType resultType;
        List<Mutable<ILogicalExpression>> args;
        Field[] fields = clazz.getFields();

        for (Field field : fields) {

            // Ensure the field is one of the instances of the class
            if (field.getType().equals(clazz)) {
                LOGGER.log(Level.INFO, "Testing " + clazz.getSimpleName() + ": " + field.getName());

                // Need to check if this is a special type computer that counts number of arguments
                args = checkArgsCountFunctions.get(field);

                // Yes, pass its specified arguments in the map
                if (args != null) {
                    when(functionCallExpression.getArguments()).thenReturn(args);
                }
                // No, pass six arguments
                else {
                    when(functionCallExpression.getArguments()).thenReturn(sixArgs);
                }

                instance = (IResultTypeComputer) field.get(null);
                resultType = instance.computeType(functionCallExpression, typeEnv, metadataProvider);
                ATypeTag typeTag = resultType.getTypeTag();

                // Result should be ANY, Missable or Nullable
                Assert.assertTrue(typeTag == ATypeTag.ANY
                        || (typeTag == ATypeTag.UNION && ((AUnionType) resultType).isNullableType()
                                || ((AUnionType) resultType).isMissableType()));
            }
        }
    }

    public static void prepare() {

        // Add to exception list for computers having a different behavior for "any" type
        addComputersBehavingDifferently();
    }

    // TODO This is not a good practice, if the class name is changed, the test will fail and this needs to be updated
    // Consider using annotation to avoid modifying the test and have a generic behavior
    /**
     * Adds the type computers that have a different behavior for "any" type.
     */
    private static void addComputersBehavingDifferently() {
        differentBehaviorFunctions.add("InjectFailureTypeComputer");
        differentBehaviorFunctions.add("RecordAddFieldsTypeComputer");
        differentBehaviorFunctions.add("OpenRecordConstructorResultType");
        differentBehaviorFunctions.add("RecordRemoveFieldsTypeComputer");
        differentBehaviorFunctions.add("ClosedRecordConstructorResultType");
        differentBehaviorFunctions.add("LocalAvgTypeComputer");
        differentBehaviorFunctions.add("BooleanOnlyTypeComputer");
        differentBehaviorFunctions.add("AMissingTypeComputer");
        differentBehaviorFunctions.add("NullableDoubleTypeComputer");
        differentBehaviorFunctions.add("RecordMergeTypeComputer");
        differentBehaviorFunctions.add("BooleanOrMissingTypeComputer");
        differentBehaviorFunctions.add("LocalSingleVarStatisticsTypeComputer");
    }

    // TODO(Hussain) Remove this after the type computers are updated
    /**
     * Adds the type computers that check the args count in their method body. If 6 arguments are passed to those
     * computers, they're gonna throw an exception, so we manually specify how many arguments they should get.
     *
     * @throws Exception Exception
     */
    private static void addComputersCheckingArgsCount(List<Mutable<ILogicalExpression>> threeArgs,
            List<Mutable<ILogicalExpression>> fourArgs) throws Exception {

        // AListTypeComputer
        Class<?> clazz = Class.forName("org.apache.asterix.om.typecomputer.impl.AListTypeComputer");
        Field[] fields = clazz.getFields();

        for (Field field : fields) {
            if (field.getName().equalsIgnoreCase("INSTANCE_SLICE")) {
                LOGGER.log(Level.INFO, field.getName() + " will use only 3 arguments");
                checkArgsCountFunctions.put(field, threeArgs);
            }

            if (field.getName().equalsIgnoreCase("INSTANCE_REPLACE")) {
                LOGGER.log(Level.INFO, field.getName() + " will use only 4 arguments");
                checkArgsCountFunctions.put(field, fourArgs);
            }
        }

        // ArrayRangeTypeComputer
        clazz = Class.forName("org.apache.asterix.om.typecomputer.impl.ArrayRangeTypeComputer");
        fields = clazz.getFields();

        for (Field field : fields) {
            if (field.getName().equalsIgnoreCase("INSTANCE")) {
                LOGGER.log(Level.INFO, field.getName() + " will use only 3 arguments");
                checkArgsCountFunctions.put(field, threeArgs);
            }
        }
    }

    /**
     * Creates expressions matching the number passed as an argument. Variable type environment is set for all those
     * expressions to be of type "any".
     *
     * @param numArgs number of arguments to create
     * @return a list holding the created expressions
     * @throws Exception Exception
     */
    private static List<Mutable<ILogicalExpression>> createArgs(int numArgs, IVariableTypeEnvironment typeEnv)
            throws Exception {

        List<Mutable<ILogicalExpression>> arguments = new ArrayList<>();

        for (int i = 0; i < numArgs; ++i) {
            ILogicalExpression argument = mock(ILogicalExpression.class);
            arguments.add(new MutableObject<>(argument));
            when(typeEnv.getType(argument)).thenReturn(BuiltinType.ANY);
        }

        return arguments;
    }
}
