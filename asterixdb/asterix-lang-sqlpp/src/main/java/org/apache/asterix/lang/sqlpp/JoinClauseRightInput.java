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
package org.apache.asterix.lang.sqlpp;

import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TimeTravel;

public class JoinClauseRightInput {
    private final Expression rightExpr;
    private final VariableExpr rightVar;
    private final VariableExpr posVar;
    private final TimeTravel timeTravel;

    public JoinClauseRightInput(Expression rightExpr, VariableExpr rightVar, VariableExpr posVar,
            TimeTravel timeTravel) {
        this.rightExpr = rightExpr;
        this.rightVar = rightVar;
        this.posVar = posVar;
        this.timeTravel = timeTravel;
    }

    public Expression getRightExpr() {
        return rightExpr;
    }

    public VariableExpr getRightVar() {
        return rightVar;
    }

    public VariableExpr getPosVar() {
        return posVar;
    }

    public TimeTravel getTimeTravel() {
        return timeTravel;
    }
}
