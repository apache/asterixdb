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

package org.apache.asterix.runtime.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.dataflow.data.common.ExpressionTypeComputer;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADayTimeDuration;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.ADuration;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.ATime;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.base.AYearMonthDuration;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.exceptions.UnsupportedTypeException;
import org.apache.asterix.om.functions.BuiltinFunctionInfo;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test alignment of type computers between aggregate functions and their scalar versions
 */
@RunWith(Parameterized.class)
public class ScalarAggregateTypeComputerTest {

    private static final IAObject[] ITEMS = {
            //
            ABoolean.TRUE,
            //
            new AInt8((byte) 0),
            //
            new AInt16((short) 0),
            //
            new AInt32(0),
            //
            new AInt64(0),
            //
            new AFloat(0),
            //
            new ADouble(0),
            //
            new AString(""),
            //
            new ADate(0),
            //
            new ADateTime(0),
            //
            new ATime(0),
            //
            new ADuration(0, 0),
            //
            new AYearMonthDuration(0),
            //
            new ADayTimeDuration(0),
            //
            new AInterval(0, 0, ATypeTag.DATETIME.serialize()),
            //
            new AOrderedList(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE, Collections.singletonList(new AString(""))),
            //
            new AUnorderedList(AUnorderedListType.FULLY_OPEN_UNORDEREDLIST_TYPE,
                    Collections.singletonList(new AString(""))),
            //
            new ARecord(
                    new ARecordType("record-type", new String[] { "a" }, new IAType[] { BuiltinType.ASTRING }, false),
                    new IAObject[] { new AString("") }) };

    // Test parameters
    @Parameterized.Parameter
    public String testName;

    @Parameterized.Parameter(1)
    public FunctionIdentifier scalarfid;

    @Parameterized.Parameter(2)
    public FunctionIdentifier aggfid;

    @Parameterized.Parameter(3)
    public IAObject item;

    @Parameterized.Parameters(name = "ScalarAggregateTypeComputerTest {index}: {0}({3})")
    public static Collection<Object[]> tests() {
        List<Object[]> tests = new ArrayList<>();

        FunctionCollection fcoll = FunctionCollection.createDefaultFunctionCollection();
        for (IFunctionDescriptorFactory fdf : fcoll.getFunctionDescriptorFactories()) {
            FunctionIdentifier fid = fdf.createFunctionDescriptor().getIdentifier();
            FunctionIdentifier aggfid = BuiltinFunctions.getAggregateFunction(fid);
            if (aggfid == null) {
                continue;
            }
            for (IAObject item : ITEMS) {
                tests.add(new Object[] { fid.getName(), fid, aggfid, item });
            }

        }
        return tests;
    }

    @Test
    public void test() throws Exception {

        AOrderedListType listType = new AOrderedListType(item.getType(), null);
        AOrderedList list = new AOrderedList(listType, Collections.singletonList(item));
        ConstantExpression scalarArgExpr = new ConstantExpression(new AsterixConstantValue(list));
        BuiltinFunctionInfo scalarfi = BuiltinFunctions.getBuiltinFunctionInfo(scalarfid);
        ScalarFunctionCallExpression scalarCallExpr =
                new ScalarFunctionCallExpression(scalarfi, new MutableObject<>(scalarArgExpr));
        IAType scalarResultType = computeType(scalarCallExpr);

        ConstantExpression aggArgExpr = new ConstantExpression(new AsterixConstantValue(item));
        BuiltinFunctionInfo aggfi = BuiltinFunctions.getBuiltinFunctionInfo(aggfid);
        AggregateFunctionCallExpression aggCallExpr = new AggregateFunctionCallExpression(aggfi, false,
                Collections.singletonList(new MutableObject<>(aggArgExpr)));
        IAType aggResultType = computeType(aggCallExpr);

        if (!compareResultTypes(scalarResultType, aggResultType)) {
            Assert.fail(String.format("%s(%s) returns %s != %s(%s) returns %s", scalarfid.getName(), item.getType(),
                    formatResultType(scalarResultType), aggfid.getName(), item.getType(),
                    formatResultType(aggResultType)));
        }
    }

    private boolean compareResultTypes(IAType t1, IAType t2) {
        // null means ERROR
        if (t1 == null) {
            // OK if both types are ERROR
            return t2 == null;
        } else if (t2 == null) {
            return false;
        }
        boolean t1Union = false, t2Union = false;
        if (t1.getTypeTag() == ATypeTag.UNION) {
            t1Union = true;
            t1 = ((AUnionType) t1).getActualType();
        }
        if (t2.getTypeTag() == ATypeTag.UNION) {
            t2Union = true;
            t2 = ((AUnionType) t2).getActualType();
        }
        return (t1Union == t2Union) && t1.deepEqual(t2);
    }

    private String formatResultType(IAType t) {
        return t == null ? "ERROR" : t.toString();
    }

    private IAType computeType(AbstractFunctionCallExpression callExpr) throws AlgebricksException {
        try {
            BuiltinFunctionInfo fi = Objects.requireNonNull((BuiltinFunctionInfo) callExpr.getFunctionInfo());
            return fi.getResultTypeComputer().computeType(callExpr, EMPTY_TYPE_ENV, null);
        } catch (UnsupportedTypeException e) {
            return null;
        }
    }

    private static final IVariableTypeEnvironment EMPTY_TYPE_ENV = new IVariableTypeEnvironment() {

        @Override
        public boolean substituteProducedVariable(LogicalVariable v1, LogicalVariable v2) {
            throw new IllegalStateException();
        }

        @Override
        public void setVarType(LogicalVariable var, Object type) {
            throw new IllegalStateException();
        }

        @Override
        public Object getVarType(LogicalVariable var, List<LogicalVariable> nonMissableVariables,
                List<List<LogicalVariable>> correlatedMissableVariableLists, List<LogicalVariable> nonNullableVariables,
                List<List<LogicalVariable>> correlatedNullableVariableLists) {
            throw new IllegalStateException();
        }

        @Override
        public Object getVarType(LogicalVariable var) {
            throw new IllegalStateException();
        }

        @Override
        public Object getType(ILogicalExpression expr) throws AlgebricksException {
            return ExpressionTypeComputer.INSTANCE.getType(expr, null, this);
        }
    };
}
