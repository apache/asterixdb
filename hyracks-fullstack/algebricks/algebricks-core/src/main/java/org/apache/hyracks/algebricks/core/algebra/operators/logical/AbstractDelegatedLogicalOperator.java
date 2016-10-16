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

import java.util.List;

import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;

/**
 * @author rico
 */
public abstract class AbstractDelegatedLogicalOperator implements IOperatorDelegate {

    private AbstractLogicalOperator.ExecutionMode mode = AbstractLogicalOperator.ExecutionMode.UNPARTITIONED;
    protected List<LogicalVariable> schema;
    protected IPhysicalOperator physicalOperator;

    @Override
    public ExecutionMode getExecutionMode() {
        return mode;
    }

    @Override
    public void setExecutionMode(ExecutionMode mode) {
        this.mode = mode;
    }

    @Override
    public void setSchema(List<LogicalVariable> schema) {
        this.schema = schema;
    }

    @Override
    public IPhysicalOperator getPhysicalOperator() {
        return physicalOperator;
    }

    @Override
    public void setPhysicalOperator(IPhysicalOperator physicalOperator) {
        this.physicalOperator = physicalOperator;
    }
}
