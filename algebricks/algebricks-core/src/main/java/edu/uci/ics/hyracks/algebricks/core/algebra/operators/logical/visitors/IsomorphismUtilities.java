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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors;

import java.util.Map;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public class IsomorphismUtilities {

    public static void mapVariablesTopDown(ILogicalOperator op, ILogicalOperator arg,
            Map<LogicalVariable, LogicalVariable> variableMapping) throws AlgebricksException {
        IsomorphismVariableMappingVisitor visitor = new IsomorphismVariableMappingVisitor(variableMapping);
        op.accept(visitor, arg);
    }

    public static boolean isOperatorIsomorphic(ILogicalOperator op, ILogicalOperator arg) throws AlgebricksException {
        IsomorphismOperatorVisitor visitor = new IsomorphismOperatorVisitor();
        return op.accept(visitor, arg).booleanValue();
    }

}
