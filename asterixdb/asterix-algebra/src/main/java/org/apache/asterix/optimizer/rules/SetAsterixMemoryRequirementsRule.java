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

package org.apache.asterix.optimizer.rules;

import java.util.Set;
import java.util.function.Predicate;

import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.base.AsterixOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import org.apache.hyracks.algebricks.rewriter.rules.SetMemoryRequirementsRule;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

/**
 * This rule extends {@link SetMemoryRequirementsRule} and modifies its functionality as follows:
 * <ul>
 * <li>It skips memory requirements configuration if the query operates only on metadata datasets and/or
 * datasource functions annotated with
 * {@link BuiltinFunctions.DataSourceFunctionProperty#MIN_MEMORY_BUDGET MIN_MEMORY_BUDGET} property.
 * In this case operators will retain their default (minimal) memory requirements.
 * </li>
 * </ul>
 */
public final class SetAsterixMemoryRequirementsRule extends SetMemoryRequirementsRule {

    @Override
    protected ILogicalOperatorVisitor<Void, Void> createMemoryRequirementsConfigurator(IOptimizationContext context) {
        return forceMinMemoryBudget((AsterixOptimizationContext) context) ? null
                : super.createMemoryRequirementsConfigurator(context);
    }

    private boolean forceMinMemoryBudget(AsterixOptimizationContext context) {
        Int2ObjectMap<Set<DataSourceId>> dataSourceMap = context.getDataSourceMap();
        if (dataSourceMap.isEmpty()) {
            return false;
        }
        for (Int2ObjectMap.Entry<Set<DataSourceId>> me : dataSourceMap.int2ObjectEntrySet()) {
            int dataSourceType = me.getIntKey();
            Predicate<DataSourceId> dataSourceTest;
            switch (dataSourceType) {
                case DataSource.Type.INTERNAL_DATASET:
                    dataSourceTest = SetAsterixMemoryRequirementsRule::isMinMemoryBudgetDataset;
                    break;
                case DataSource.Type.FUNCTION:
                    dataSourceTest = SetAsterixMemoryRequirementsRule::isMinMemoryBudgetFunction;
                    break;
                default:
                    return false;
            }
            if (!me.getValue().stream().allMatch(dataSourceTest)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isMinMemoryBudgetDataset(DataSourceId dsId) {
        return MetadataConstants.METADATA_DATAVERSE_NAME.equals(dsId.getDataverseName());
    }

    private static boolean isMinMemoryBudgetFunction(DataSourceId dsId) {
        return BuiltinFunctions.builtinFunctionHasProperty(
                new FunctionIdentifier(dsId.getDataverseName(), dsId.getDatasourceName()),
                BuiltinFunctions.DataSourceFunctionProperty.MIN_MEMORY_BUDGET);
    }
}