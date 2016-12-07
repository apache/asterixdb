
/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.asterix.om.typecomputer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
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
import org.junit.Assert;
import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

public class ExceptionTest {

    @Test
    public void test() throws Exception {
        // Tests all usual type computers.
        Reflections reflections = new Reflections("org.apache.asterix.om.typecomputer", new SubTypesScanner(false));
        Set<Class<? extends IResultTypeComputer>> classes = reflections.getSubTypesOf(IResultTypeComputer.class);
        int numTypeComputers = 0;
        for (Class<? extends IResultTypeComputer> c : classes) {
            if (Modifier.isAbstract(c.getModifiers())) {
                continue;
            }
            testTypeComputer(c);
            ++numTypeComputers;
        }
        // Currently, there are 78 type computers.
        Assert.assertTrue(numTypeComputers >= 78);
    }

    private void testTypeComputer(Class<? extends IResultTypeComputer> c) throws Exception {
        // Mocks the type environment.
        IVariableTypeEnvironment mockTypeEnv = mock(IVariableTypeEnvironment.class);
        // Mocks the metadata provider.
        IMetadataProvider<?, ?> mockMetadataProvider = mock(IMetadataProvider.class);

        // Mocks function expression.
        AbstractFunctionCallExpression mockExpr = mock(AbstractFunctionCallExpression.class);
        FunctionIdentifier fid = mock(FunctionIdentifier.class);
        when(mockExpr.getFunctionIdentifier()).thenReturn(fid);
        when(fid.getName()).thenReturn("testFunction");

        int numCombination = (int) Math.pow(ATypeTag.values().length, 2);
        // Sets two arguments for the mocked function expression.
        for (int index = 0; index < numCombination; ++index) {
            try {
                List<Mutable<ILogicalExpression>> argRefs = new ArrayList<>();
                for (int argIndex = 0; argIndex < 2; ++argIndex) {
                    int base = (int) Math.pow(ATypeTag.values().length, argIndex);
                    ILogicalExpression mockArg = mock(ILogicalExpression.class);
                    argRefs.add(new MutableObject<>(mockArg));
                    IAType mockType = mock(IAType.class);
                    when(mockTypeEnv.getType(mockArg)).thenReturn(mockType);
                    int serializedTypeTag = (index / base) % ATypeTag.values().length + 1;
                    ATypeTag typeTag = ATypeTag.VALUE_TYPE_MAPPING[serializedTypeTag];
                    if (typeTag == null) {
                        // For some reason, type tag 39 does not exist.
                        typeTag = ATypeTag.ANY;
                    }
                    when(mockType.getTypeTag()).thenReturn(typeTag);
                }

                // Sets up arguments for the mocked expression.
                when(mockExpr.getArguments()).thenReturn(argRefs);

                // Sets up required/actual types of the mocked expression.
                Object[] opaqueParameters = new Object[2];
                opaqueParameters[0] = BuiltinType.ANY;
                opaqueParameters[1] = BuiltinType.ANY;
                when(mockExpr.getOpaqueParameters()).thenReturn(opaqueParameters);

                // Invokes a type computer.
                IResultTypeComputer instance = (IResultTypeComputer) c.getField("INSTANCE").get(null);
                instance.computeType(mockExpr, mockTypeEnv, mockMetadataProvider);
            } catch (AlgebricksException ae) {
                String msg = ae.getMessage();
                if (msg.startsWith("ASX")) {
                    // Verifies the error code.
                    int errorCode = Integer.parseInt(msg.substring(3, 7));
                    Assert.assertTrue(errorCode >= 1000 && errorCode < 2000);
                    continue;
                } else {
                    // Any root-level compilation exceptions thrown from type computers should have an error code.
                    Assert.assertTrue(!(ae instanceof AlgebricksException) || (ae.getCause() != null));
                }
            } catch (ClassCastException e) {
                continue;
            }
        }
    }
}
