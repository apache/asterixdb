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

package org.apache.asterix.optimizer.base;

import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.core.algebra.expressions.IConflictingTypeResolver;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.rewriter.base.AlgebricksOptimizationContext;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.exceptions.IWarningCollector;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public final class AsterixOptimizationContext extends AlgebricksOptimizationContext {

    private final Int2ObjectMap<Set<DataSourceId>> dataSourceMap = new Int2ObjectOpenHashMap<>();

    public AsterixOptimizationContext(int varCounter, IExpressionEvalSizeComputer expressionEvalSizeComputer,
            IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
            IExpressionTypeComputer expressionTypeComputer, IMissableTypeComputer nullableTypeComputer,
            IConflictingTypeResolver conflictingTypeResovler, PhysicalOptimizationConfig physicalOptimizationConfig,
            AlgebricksPartitionConstraint clusterLocations, IPlanPrettyPrinter prettyPrinter,
            IWarningCollector warningCollector) {
        super(varCounter, expressionEvalSizeComputer, mergeAggregationExpressionFactory, expressionTypeComputer,
                nullableTypeComputer, conflictingTypeResovler, physicalOptimizationConfig, clusterLocations,
                prettyPrinter, warningCollector);
    }

    public void addDataSource(DataSource dataSource) {
        byte type = dataSource.getDatasourceType();
        Set<DataSourceId> set = dataSourceMap.get(type);
        if (set == null) {
            set = new HashSet<>();
            dataSourceMap.put(type, set);
        }
        set.add(dataSource.getId());
    }

    public Int2ObjectMap<Set<DataSourceId>> getDataSourceMap() {
        return dataSourceMap;
    }
}
