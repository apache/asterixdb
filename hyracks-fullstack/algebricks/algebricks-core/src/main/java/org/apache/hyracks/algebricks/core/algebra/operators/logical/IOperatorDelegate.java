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

import java.util.Collection;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;

/**
 * @author rico
 */
public interface IOperatorDelegate {

    void setExecutionMode(ExecutionMode mode);

    boolean isMap();

    public IOperatorDelegate newInstance();

    boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform) throws AlgebricksException;

    void setSchema(List<LogicalVariable> schema);

    IPhysicalOperator getPhysicalOperator();

    void setPhysicalOperator(IPhysicalOperator physicalOperator);

    ExecutionMode getExecutionMode();

    public void getUsedVariables(Collection<LogicalVariable> usedVars);

    public void getProducedVariables(Collection<LogicalVariable> producedVars);
}
