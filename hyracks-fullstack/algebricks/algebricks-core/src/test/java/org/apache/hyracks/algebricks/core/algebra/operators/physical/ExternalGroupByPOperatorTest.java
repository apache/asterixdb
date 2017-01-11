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

package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.junit.Assert;
import org.junit.Test;

import junit.extensions.PA;

public class ExternalGroupByPOperatorTest {

    @Test
    public void testCalculateGroupByTableCardinality() throws Exception {

        // Creates a dummy variable and an expression that are needed by the operator. They are not used by this test.
        LogicalVariable v = new LogicalVariable(0);
        MutableObject<ILogicalExpression> e = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v));
        List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyList = new ArrayList<>();
        gbyList.add(new Pair<>(v, e));
        ExternalGroupByPOperator eGByOp = new ExternalGroupByPOperator(gbyList, 0, 0);

        // Test 1: compiler.groupmemory: 512 bytes, frame size: 256 bytes, with 1 column group-by
        long memoryBudgetInBytes = 512;
        int numberOfGroupByColumns = 1;
        int frameSize = 256;
        int resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 9);

        // Sets the frame size to 128KB.
        frameSize = 128 * 1024;

        // Test 2: memory size: 1 MB, frame size: 128 KB, 1 column group-by
        memoryBudgetInBytes = 1024 * 1024;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 19660);

        // Test 3: memory size: 100 MB, frame size: 128 KB, 1 column group-by
        memoryBudgetInBytes = 1024 * 1024 * 100;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 1937883);

        // Test 4: memory size: 1 GB, frame size: 128 KB, 1 column group-by
        memoryBudgetInBytes = 1024 * 1024 * 1024;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 19841178);

        // Test 5: memory size: 10 GB, frame size: 128 KB, 1 column group-by
        memoryBudgetInBytes = 1024 * 1024 * 1024 * 10L;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 198409112);

        // Test 6: memory size: 100 GB, frame size: 128 KB, 1 column group-by
        memoryBudgetInBytes = 1024 * 1024 * 1024 * 100L;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 1962753871);

        // Test 7: memory size: 1 TB, frame size: 128 KB, 1 column group-by
        // The cardinality will be set to Integer.MAX_VALUE in this case since the budget is too huge.
        memoryBudgetInBytes = 1024 * 1024 * 1024 * 1024L;
        frameSize = 128 * 1024;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 2147483647);

        // Test 8: memory size: 1 MB, frame size: 128 KB, 2 columns group-by
        memoryBudgetInBytes = 1024 * 1024;
        numberOfGroupByColumns = 2;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 16681);

        // Test 9: memory size: 1 MB, frame size: 128 KB, 3 columns group-by
        memoryBudgetInBytes = 1024 * 1024;
        numberOfGroupByColumns = 3;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 15176);

        // Test 10: memory size: 1 MB, frame size: 128 KB, 4 columns group-by
        memoryBudgetInBytes = 1024 * 1024;
        numberOfGroupByColumns = 4;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 13878);

        // Test 11: memory size: 32 MB, frame size: 128 KB, 2 columns group-by
        memoryBudgetInBytes = 1024 * 1024 * 32L;
        numberOfGroupByColumns = 4;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 408503);
    }

}
