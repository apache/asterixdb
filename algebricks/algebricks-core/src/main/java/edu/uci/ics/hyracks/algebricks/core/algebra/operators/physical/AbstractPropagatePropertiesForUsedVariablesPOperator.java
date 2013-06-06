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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;

public abstract class AbstractPropagatePropertiesForUsedVariablesPOperator extends AbstractPhysicalOperator {

    public void computeDeliveredPropertiesForUsedVariables(ILogicalOperator op, List<LogicalVariable> usedVariables) {
        ILogicalOperator op2 = op.getInputs().get(0).getValue();
        IPartitioningProperty pp = op2.getDeliveredPhysicalProperties().getPartitioningProperty();
        List<ILocalStructuralProperty> downPropsLocal = op2.getDeliveredPhysicalProperties().getLocalProperties();
        List<ILocalStructuralProperty> propsLocal = new ArrayList<ILocalStructuralProperty>();
        for (ILocalStructuralProperty lsp : downPropsLocal) {
            LinkedList<LogicalVariable> cols = new LinkedList<LogicalVariable>();
            lsp.getColumns(cols);
            if (usedVariables.containsAll(cols)) {
                propsLocal.add(lsp);
            }
        }
        deliveredProperties = new StructuralPropertiesVector(pp, propsLocal);
    }

}
