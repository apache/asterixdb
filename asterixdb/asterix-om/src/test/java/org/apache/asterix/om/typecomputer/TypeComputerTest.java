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

package org.apache.asterix.om.typecomputer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
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
import org.junit.Assert;
import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

// Tests if all type computers can handle input type ANY properly.
public class TypeComputerTest {

    @Test
    public void test() throws Exception {
        // Mocks the type environment.
        IVariableTypeEnvironment mockTypeEnv = mock(IVariableTypeEnvironment.class);

        // Mocks the metadata provider.
        IMetadataProvider<?, ?> mockMetadataProvider = mock(IMetadataProvider.class);

        // Mocks function expression.
        AbstractFunctionCallExpression mockExpr = mock(AbstractFunctionCallExpression.class);
        FunctionIdentifier fid = mock(FunctionIdentifier.class);
        when(mockExpr.getFunctionIdentifier()).thenReturn(fid);
        when(fid.getName()).thenReturn("testFunction");

        // A function at most has six argument.
        List<Mutable<ILogicalExpression>> sixArgs = createArgs(6, mockTypeEnv);

        // Sets up arguments for the mocked expression.
        when(mockExpr.getArguments()).thenReturn(sixArgs);

        // Sets up required/actual types of the mocked expression.
        Object[] opaqueParameters = new Object[2];
        opaqueParameters[0] = BuiltinType.ANY;
        opaqueParameters[1] = BuiltinType.ANY;
        when(mockExpr.getOpaqueParameters()).thenReturn(opaqueParameters);

        // functions that check the number of args inside the type computer
        List<Mutable<ILogicalExpression>> replaceArgs = createArgs(4, mockTypeEnv);
        List<Mutable<ILogicalExpression>> sliceArgs = createArgs(3, mockTypeEnv);
        List<Mutable<ILogicalExpression>> rangeArgs = createArgs(3, mockTypeEnv);
        HashMap<String, List<Mutable<ILogicalExpression>>> map = new HashMap<>();
        map.put("INSTANCE_REPLACE", replaceArgs);
        map.put("INSTANCE_SLICE", sliceArgs);
        map.put("ArrayRangeTypeComputer", rangeArgs);

        // Several exceptional type computers.
        Set<String> exceptionalTypeComputers = new HashSet<>();
        exceptionalTypeComputers.add("InjectFailureTypeComputer");
        exceptionalTypeComputers.add("RecordAddFieldsTypeComputer");
        exceptionalTypeComputers.add("OpenRecordConstructorResultType");
        exceptionalTypeComputers.add("RecordRemoveFieldsTypeComputer");
        exceptionalTypeComputers.add("ClosedRecordConstructorResultType");
        exceptionalTypeComputers.add("LocalAvgTypeComputer");
        exceptionalTypeComputers.add("BooleanOnlyTypeComputer");
        exceptionalTypeComputers.add("AMissingTypeComputer");
        exceptionalTypeComputers.add("NullableDoubleTypeComputer");
        exceptionalTypeComputers.add("RecordMergeTypeComputer");
        exceptionalTypeComputers.add("BooleanOrMissingTypeComputer");
        exceptionalTypeComputers.add("LocalSingleVarStatisticsTypeComputer");

        // Tests all usual type computers.
        Reflections reflections = new Reflections("org.apache.asterix.om.typecomputer", new SubTypesScanner(false));
        Set<Class<? extends IResultTypeComputer>> classes = reflections.getSubTypesOf(IResultTypeComputer.class);
        for (Class<? extends IResultTypeComputer> c : classes) {
            if (exceptionalTypeComputers.contains(c.getSimpleName()) || Modifier.isAbstract(c.getModifiers())) {
                continue;
            }
            System.out.println("Test type computer: " + c.getName());
            Assert.assertTrue(testTypeComputer(c, mockTypeEnv, mockMetadataProvider, mockExpr, map, sixArgs));
        }
    }

    private boolean testTypeComputer(Class<? extends IResultTypeComputer> c, IVariableTypeEnvironment mockTypeEnv,
            IMetadataProvider<?, ?> mockMetadataProvider, AbstractFunctionCallExpression mockExpr,
            HashMap<String, List<Mutable<ILogicalExpression>>> map, List<Mutable<ILogicalExpression>> sixArgs)
            throws Exception {
        // Tests the return type. It should be either ANY or NULLABLE/MISSABLE.
        IResultTypeComputer instance;
        IAType resultType;
        Field[] fields = c.getFields();
        List<Mutable<ILogicalExpression>> args;
        for (Field field : fields) {
            if (field.getName().startsWith("INSTANCE")) {
                System.out.println("Test type computer INSTANCE: " + field.getName());
                args = getArgs(field.getName(), c, map);
                if (args != null) {
                    when(mockExpr.getArguments()).thenReturn(args);
                } else {
                    when(mockExpr.getArguments()).thenReturn(sixArgs);
                }
                instance = (IResultTypeComputer) field.get(null);
                resultType = instance.computeType(mockExpr, mockTypeEnv, mockMetadataProvider);
                ATypeTag typeTag = resultType.getTypeTag();
                if (typeTag != ATypeTag.ANY && !(typeTag == ATypeTag.UNION && ((AUnionType) resultType).isNullableType()
                        && ((AUnionType) resultType).isMissableType())) {
                    return false;
                }
            }
        }
        return true;
    }

    private List<Mutable<ILogicalExpression>> createArgs(int numArgs, IVariableTypeEnvironment mockTypeEnv)
            throws Exception {
        List<Mutable<ILogicalExpression>> argRefs = new ArrayList<>();
        for (int argIndex = 0; argIndex < numArgs; ++argIndex) {
            ILogicalExpression mockArg = mock(ILogicalExpression.class);
            argRefs.add(new MutableObject<>(mockArg));
            when(mockTypeEnv.getType(mockArg)).thenReturn(BuiltinType.ANY);
        }

        return argRefs;
    }

    private List<Mutable<ILogicalExpression>> getArgs(String instanceName, Class<? extends IResultTypeComputer> c,
            HashMap<String, List<Mutable<ILogicalExpression>>> map) {
        if (instanceName.equals("INSTANCE")) {
            return map.get(c.getSimpleName());
        } else {
            return map.get(instanceName);
        }
    }
}
