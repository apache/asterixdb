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
package org.apache.asterix.metadata.utils.filter;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluatorFactory;
import org.apache.asterix.column.filter.iterable.accessor.ColumnFilterValueAccessorEvaluatorFactory;
import org.apache.asterix.column.filter.iterable.evaluator.ColumnIterableFilterEvaluatorFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.ColumnDatasetProjectionFiltrationInfo;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class ColumnFilterBuilder extends AbstractFilterBuilder {

    public ColumnFilterBuilder(ColumnDatasetProjectionFiltrationInfo projectionFiltrationInfo, JobGenContext context,
            IVariableTypeEnvironment typeEnv) {
        super(projectionFiltrationInfo.getFilterPaths(), projectionFiltrationInfo.getFilterExpression(), context,
                typeEnv);
    }

    public IColumnIterableFilterEvaluatorFactory build() throws AlgebricksException {
        if (filterExpression == null || filterPaths.isEmpty()) {
            return NoOpColumnFilterEvaluatorFactory.INSTANCE;
        }
        // Each conjunct of a top-level AND is evaluated alone, so that a component can drop the ones it cannot filter.
        List<Mutable<ILogicalExpression>> conjuncts = new ArrayList<>();
        if (!filterExpression.splitIntoConjuncts(conjuncts)) {
            conjuncts.add(new MutableObject<>(filterExpression));
        }
        IScalarEvaluatorFactory[] conjunctFactories = new IScalarEvaluatorFactory[conjuncts.size()];
        for (int i = 0; i < conjunctFactories.length; i++) {
            conjunctFactories[i] = createEvaluator(conjuncts.get(i).getValue());
            if (conjunctFactories[i] == null) {
                return NoOpColumnFilterEvaluatorFactory.INSTANCE;
            }
        }
        return new ColumnIterableFilterEvaluatorFactory(conjunctFactories);
    }

    @Override
    protected IScalarEvaluatorFactory createValueAccessor(ILogicalExpression expression) {
        ARecordType path = filterPaths.get(expression);
        return new ColumnFilterValueAccessorEvaluatorFactory(path);
    }
}
