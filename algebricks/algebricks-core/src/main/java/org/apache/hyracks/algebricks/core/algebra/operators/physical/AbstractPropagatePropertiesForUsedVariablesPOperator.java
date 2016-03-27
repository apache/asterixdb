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
package org.apache.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;

public abstract class AbstractPropagatePropertiesForUsedVariablesPOperator extends AbstractPhysicalOperator {

    public void computeDeliveredPropertiesForUsedVariables(ILogicalOperator op, List<LogicalVariable> usedVariables) {
        ILogicalOperator op2 = op.getInputs().get(0).getValue();
        IPartitioningProperty pp = op2.getDeliveredPhysicalProperties().getPartitioningProperty();
        List<ILocalStructuralProperty> downPropsLocal = op2.getDeliveredPhysicalProperties().getLocalProperties();
        List<ILocalStructuralProperty> propsLocal = new ArrayList<ILocalStructuralProperty>();
        if (downPropsLocal != null) {
            for (ILocalStructuralProperty lsp : downPropsLocal) {
                LinkedList<LogicalVariable> cols = new LinkedList<LogicalVariable>();
                lsp.getColumns(cols);
                ILocalStructuralProperty propagatedProp = lsp.retainVariables(usedVariables);
                if (propagatedProp != null) {
                    propsLocal.add(propagatedProp);
                }
            }
        }
        deliveredProperties = new StructuralPropertiesVector(pp, propsLocal);
    }
}
