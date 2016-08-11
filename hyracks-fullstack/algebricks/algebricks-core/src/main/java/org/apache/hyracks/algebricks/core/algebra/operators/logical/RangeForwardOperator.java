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
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.api.dataflow.value.IRangeMap;
import org.apache.hyracks.dataflow.std.base.RangeId;

public class RangeForwardOperator extends AbstractLogicalOperator {

    private RangeId rangeId;
    private IRangeMap rangeMap;

    public RangeForwardOperator(RangeId rangeId, IRangeMap rangeMap) {
        this.rangeId = rangeId;
        this.rangeMap = rangeMap;
    }

    public RangeId getRangeId() {
        return rangeId;
    }

    public IRangeMap getRangeMap() {
        return rangeMap;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.RANGE_FORWARD;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitRangeForwardOperator(this, arg);
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createPropagatingAllInputsTypeEnvironment(ctx);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        schema = new ArrayList<LogicalVariable>(inputs.get(0).getValue().getSchema());
    }

}
