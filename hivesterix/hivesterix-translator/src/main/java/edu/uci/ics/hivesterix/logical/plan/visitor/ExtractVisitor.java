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
package edu.uci.ics.hivesterix.logical.plan.visitor;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;

import edu.uci.ics.hivesterix.logical.plan.visitor.base.DefaultVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.Translator;
import edu.uci.ics.hivesterix.runtime.jobgen.Schema;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public class ExtractVisitor extends DefaultVisitor {

    @Override
    public Mutable<ILogicalOperator> visit(ExtractOperator operator,
            Mutable<ILogicalOperator> AlgebricksParentOperatorRef, Translator t) {
        Schema currentSchema = t.generateInputSchema(operator.getParentOperators().get(0));
        operator.setSchema(operator.getParentOperators().get(0).getSchema());
        List<LogicalVariable> latestOutputSchema = t.getVariablesFromSchema(currentSchema);
        t.rewriteOperatorOutputSchema(latestOutputSchema, operator);
        return null;
    }

}
