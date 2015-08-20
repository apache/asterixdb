/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical;

import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;

/**
 * @author rico
 */
public abstract class AbstractExtensibleLogicalOperator implements IOperatorExtension {

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