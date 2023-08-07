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

import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.common.external.NoOpExternalFilterEvaluatorFactory;
import org.apache.asterix.external.input.filter.ExternalFilterEvaluatorFactory;
import org.apache.asterix.external.input.filter.ExternalFilterValueEvaluatorFactory;
import org.apache.asterix.external.util.ExternalDataPrefix;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.ExternalDatasetProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class ExternalFilterBuilder extends AbstractFilterBuilder {
    private final ExternalDataPrefix prefix;

    public ExternalFilterBuilder(ExternalDatasetProjectionFiltrationInfo projectionFiltrationInfo,
            JobGenContext context, IVariableTypeEnvironment typeEnv, ExternalDataPrefix prefix) {
        super(projectionFiltrationInfo.getFilterPaths(), projectionFiltrationInfo.getFilterExpression(), context,
                typeEnv);
        this.prefix = prefix;
    }

    public IExternalFilterEvaluatorFactory build() throws AlgebricksException {
        if (filterExpression == null || filterPaths.isEmpty()) {
            return NoOpExternalFilterEvaluatorFactory.INSTANCE;
        }
        IScalarEvaluatorFactory evalFactory = createEvaluator(filterExpression);
        if (evalFactory == null) {
            return NoOpExternalFilterEvaluatorFactory.INSTANCE;
        }
        return new ExternalFilterEvaluatorFactory(prefix.getComputedFieldNames().size(), evalFactory);
    }

    @Override
    protected IScalarEvaluatorFactory createValueAccessor(ILogicalExpression expression) {
        ARecordType path = filterPaths.get(expression);
        int index = prefix.getPaths().indexOf(path);
        return new ExternalFilterValueEvaluatorFactory(index, prefix.getComputedFieldTypes().get(index));
    }
}
