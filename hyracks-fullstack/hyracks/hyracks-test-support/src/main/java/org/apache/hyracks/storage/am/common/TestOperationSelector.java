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

package org.apache.hyracks.storage.am.common;

import org.apache.hyracks.storage.am.common.datagen.ProbabilityHelper;

public class TestOperationSelector {

    public static enum TestOperation {
        INSERT,
        DELETE,
        UPDATE,
        UPSERT,
        POINT_SEARCH,
        RANGE_SEARCH,
        SCAN,
        DISKORDER_SCAN,
        MERGE
    }

    private final TestOperation[] ops;
    private final int[] cumulIntRanges;

    public TestOperationSelector(TestOperation[] ops, double[] opProbs) {
        sanityCheck(ops, opProbs);
        this.ops = ops;
        this.cumulIntRanges = ProbabilityHelper.getCumulIntRanges(opProbs);
    }

    private void sanityCheck(TestOperation[] ops, double[] opProbs) {
        if (ops.length == 0) {
            throw new RuntimeException("Empty op array.");
        }
        if (opProbs.length == 0) {
            throw new RuntimeException("Empty op probabilities.");
        }
        if (ops.length != opProbs.length) {
            throw new RuntimeException("Ops and op probabilities have unequal length.");
        }
        float sum = 0.0f;
        for (int i = 0; i < opProbs.length; i++) {
            sum += opProbs[i];
        }
        if (sum != 1.0f) {
            throw new RuntimeException("Op probabilities don't add up to 1.");
        }
    }

    public TestOperation getOp(int randomInt) {
        int ix = ProbabilityHelper.choose(cumulIntRanges, randomInt);
        return ops[ix];
    }
}
