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

import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.EMPTY_TYPE;
import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.getMergedPathRecordType;
import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.renameType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.input.filter.ExternalFilterEvaluatorFactory;
import org.apache.asterix.external.input.filter.ExternalFilterValueEvaluatorFactory;
import org.apache.asterix.external.input.filter.NoOpExternalFilterEvaluatorFactory;
import org.apache.asterix.external.util.ExternalDataPrefix;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.asterix.runtime.projection.ExternalDatasetProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;

public class ExternalFilterBuilder extends AbstractFilterBuilder {
    private final ExternalDataPrefix prefix;
    private final boolean embedFilterValues;

    public ExternalFilterBuilder(ExternalDatasetProjectionFiltrationInfo projectionFiltrationInfo,
            JobGenContext context, IVariableTypeEnvironment typeEnv, ExternalDataPrefix prefix) {
        super(projectionFiltrationInfo.getFilterPaths(), projectionFiltrationInfo.getFilterExpression(), context,
                typeEnv);
        this.prefix = prefix;
        this.embedFilterValues = projectionFiltrationInfo.isEmbedFilterValues();
    }

    public IExternalFilterEvaluatorFactory build() throws AlgebricksException {
        IScalarEvaluatorFactory evalFactory = null;
        if (filterExpression != null && !filterPaths.isEmpty()) {
            evalFactory = createEvaluator(filterExpression);
        }

        if (evalFactory == null && !embedFilterValues) {
            return NoOpExternalFilterEvaluatorFactory.INSTANCE;
        }

        List<String> fieldPaths = prefix.getComputedFieldNames();
        int numberOfComputedFields = fieldPaths.size();
        List<ProjectionFiltrationTypeUtil.RenamedType> leafs = new ArrayList<>(numberOfComputedFields);
        ARecordType previousType = EMPTY_TYPE;
        for (int i = 0; i < numberOfComputedFields; i++) {
            ProjectionFiltrationTypeUtil.RenamedType renamedType = renameType(BuiltinType.ANY, i);
            leafs.add(renamedType);
            List<String> path = Arrays.asList(fieldPaths.get(i).split("\\."));
            previousType = getMergedPathRecordType(previousType, path, renamedType);
        }

        return new ExternalFilterEvaluatorFactory(evalFactory, previousType, leafs, prefix, embedFilterValues);
    }

    @Override
    protected IScalarEvaluatorFactory createValueAccessor(ILogicalExpression expression) {
        ARecordType path = filterPaths.get(expression);
        int index = prefix.getPaths().indexOf(path);
        return new ExternalFilterValueEvaluatorFactory(index, prefix.getComputedFieldTypes().get(index));
    }
}
