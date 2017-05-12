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

package org.apache.hyracks.dataflow.std.group.external;

import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobSpecification;
import org.junit.Assert;
import org.junit.Test;

import junit.extensions.PA;

public class ExternalGroupOperatorDescriptorTest {

    @Test
    public void testCalculateGroupByTableCardinality() throws Exception {

        // Sets a dummy variable.
        IOperatorDescriptorRegistry spec = new JobSpecification(32768);
        ExternalGroupOperatorDescriptor eGByOp =
                new ExternalGroupOperatorDescriptor(spec, 0, 0, null, 4, null, null, null, null, null, null, null);

        // Test 1: compiler.groupmemory: 512 bytes, frame size: 256 bytes, with 1 column group-by
        long memoryBudgetInBytes = 512;
        int numberOfGroupByColumns = 1;
        int frameSize = 256;
        int resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 10);

        // Sets the frame size to 128KB.
        frameSize = 128 * 1024;

        // Test 2: memory size: 1 MB, frame size: 128 KB, 1 column group-by
        memoryBudgetInBytes = 1024 * 1024;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 20388);

        // Test 3: memory size: 100 MB, frame size: 128 KB, 1 column group-by
        memoryBudgetInBytes = 1024 * 1024 * 100;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 2016724);

        // Test 4: memory size: 1 GB, frame size: 128 KB, 1 column group-by
        memoryBudgetInBytes = 1024 * 1024 * 1024;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 20649113);

        // Test 5: memory size: 10 GB, frame size: 128 KB, 1 column group-by
        memoryBudgetInBytes = 1024 * 1024 * 1024 * 10L;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 206489044);

        // Test 6: memory size: 100 GB, frame size: 128 KB, 1 column group-by
        memoryBudgetInBytes = 1024 * 1024 * 1024 * 100L;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 2045222521);

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
        Assert.assertTrue(resultCardinality == 17825);

        // Test 9: memory size: 1 MB, frame size: 128 KB, 3 columns group-by
        memoryBudgetInBytes = 1024 * 1024;
        numberOfGroupByColumns = 3;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 16227);

        // Test 10: memory size: 1 MB, frame size: 128 KB, 4 columns group-by
        memoryBudgetInBytes = 1024 * 1024;
        numberOfGroupByColumns = 4;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 14563);

        // Test 11: memory size: 32 MB, frame size: 128 KB, 2 columns group-by
        memoryBudgetInBytes = 1024 * 1024 * 32L;
        numberOfGroupByColumns = 4;
        resultCardinality = (int) PA.invokeMethod(eGByOp, "calculateGroupByTableCardinality(long,int,int)",
                memoryBudgetInBytes, numberOfGroupByColumns, frameSize);
        Assert.assertTrue(resultCardinality == 441913);
    }

}
