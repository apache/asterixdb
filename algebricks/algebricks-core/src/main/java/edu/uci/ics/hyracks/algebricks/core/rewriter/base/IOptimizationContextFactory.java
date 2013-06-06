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
package edu.uci.ics.hyracks.algebricks.core.rewriter.base;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;

public interface IOptimizationContextFactory {
    public IOptimizationContext createOptimizationContext(int varCounter,
            IExpressionEvalSizeComputer expressionEvalSizeComputer,
            IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
            IExpressionTypeComputer expressionTypeComputer, INullableTypeComputer nullableTypeComputer,
            PhysicalOptimizationConfig physicalOptimizationConfig);
}
