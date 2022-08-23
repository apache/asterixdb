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

package org.apache.asterix.optimizer.rules.cbo;

import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class JoinCondition {

    public static final int NO_JC = -1;

    public ILogicalExpression joinCondition;
    public boolean derived = false;
    public boolean partOfComposite = false;
    public int numberOfVars = 0; // how many variables
    public int componentNumber = 0; // for identifying if join graph is connected
    public int datasetBits;
    // used for triangle detection; we strictly do not mean left and right here.
    // first and second sides would be more appropriate
    public int leftSideBits;
    public int rightSideBits;
    public double selectivity;
    public comparisonOp comparisonType;

    public enum comparisonOp {
        OP_EQ,
        OP_OTHER
    }

    public ILogicalExpression getJoinCondition() {
        return joinCondition;
    }
}
