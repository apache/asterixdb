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

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;

public class JoinCondition {

    protected static final int NO_JC = -1;

    protected ILogicalExpression joinCondition;
    protected boolean outerJoin;
    protected boolean derived = false;
    protected boolean partOfComposite = false;
    protected boolean deleted = false;
    protected int numberOfVars = 0; // how many variables
    protected int componentNumber = 0; // for identifying if join graph is connected
    protected int datasetBits;
    // used for triangle detection; we strictly do not mean left and right here.
    // first and second sides would be more appropriate
    protected int leftSide;
    protected int rightSide;
    protected int leftSideBits;
    protected int rightSideBits;
    protected int numLeafInputs;
    protected double selectivity = -1.0; // This must be changed obviously
    protected comparisonOp comparisonType;
    protected JoinOperator joinOp = null;
    protected List<LogicalVariable> usedVars = null;
    protected List<SelectOperator> derivedSelOps = new ArrayList<>(); // only one of them will be regarded as original

    protected enum comparisonOp {
        OP_EQ,
        OP_OTHER
    }

    protected ILogicalExpression getJoinCondition() {
        return joinCondition;
    }
}
