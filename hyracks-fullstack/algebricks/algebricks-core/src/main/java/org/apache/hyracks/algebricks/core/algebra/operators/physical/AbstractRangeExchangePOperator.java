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

import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.range.DynamicRangeMapSupplier;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMapSupplier;
import org.apache.hyracks.dataflow.common.data.partition.range.StaticRangeMapSupplier;

abstract class AbstractRangeExchangePOperator extends AbstractExchangePOperator {

    protected final List<OrderColumn> partitioningFields;

    protected final INodeDomain domain;

    protected final RangeMap rangeMap;

    protected final boolean rangeMapIsComputedAtRunTime;

    protected final String rangeMapKeyInContext;

    private AbstractRangeExchangePOperator(List<OrderColumn> partitioningFields, INodeDomain domain, RangeMap rangeMap,
            boolean rangeMapIsComputedAtRunTime, String rangeMapKeyInContext) {
        this.partitioningFields = partitioningFields;
        this.domain = domain;
        this.rangeMap = rangeMap;
        this.rangeMapIsComputedAtRunTime = rangeMapIsComputedAtRunTime;
        this.rangeMapKeyInContext = rangeMapKeyInContext;
    }

    protected AbstractRangeExchangePOperator(List<OrderColumn> partitioningFields, INodeDomain domain,
            String rangeMapKeyInContext) {
        this(partitioningFields, domain, null, true, rangeMapKeyInContext);
    }

    protected AbstractRangeExchangePOperator(List<OrderColumn> partitioningFields, INodeDomain domain,
            RangeMap rangeMap) {
        this(partitioningFields, domain, rangeMap, false, "");
    }

    public final List<OrderColumn> getPartitioningFields() {
        return partitioningFields;
    }

    public final INodeDomain getDomain() {
        return domain;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + " " + partitioningFields
                + (rangeMap != null ? " RANGE_MAP:" + rangeMap : "");
    }

    protected final RangeMapSupplier crateRangeMapSupplier() {
        return rangeMapIsComputedAtRunTime ? new DynamicRangeMapSupplier(rangeMapKeyInContext)
                : new StaticRangeMapSupplier(rangeMap);
    }

    protected Pair<int[], IBinaryComparatorFactory[]> createOrderColumnsAndComparators(ILogicalOperator op,
            IOperatorSchema opSchema, JobGenContext context) throws AlgebricksException {
        int n = partitioningFields.size();
        int[] sortFields = new int[n];
        IBinaryComparatorFactory[] comps = new IBinaryComparatorFactory[n];
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        int i = 0;
        for (OrderColumn oc : partitioningFields) {
            LogicalVariable var = oc.getColumn();
            sortFields[i] = opSchema.findVariable(var);
            Object type = env.getVarType(var);
            IBinaryComparatorFactoryProvider bcfp = context.getBinaryComparatorFactoryProvider();
            comps[i] = bcfp.getBinaryComparatorFactory(type, oc.getOrder() == OrderOperator.IOrder.OrderKind.ASC);
            i++;
        }
        return new Pair<>(sortFields, comps);
    }

    protected Triple<int[], IBinaryComparatorFactory[], INormalizedKeyComputerFactory> createOrderColumnsAndComparatorsWithNormKeyComputer(
            ILogicalOperator op, IOperatorSchema opSchema, JobGenContext context) throws AlgebricksException {
        int n = partitioningFields.size();
        int[] sortFields = new int[n];
        IBinaryComparatorFactory[] comps = new IBinaryComparatorFactory[n];
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        INormalizedKeyComputerFactoryProvider nkcfProvider = context.getNormalizedKeyComputerFactoryProvider();
        INormalizedKeyComputerFactory nkcf = null;
        int i = 0;
        for (OrderColumn oc : partitioningFields) {
            LogicalVariable var = oc.getColumn();
            sortFields[i] = opSchema.findVariable(var);
            Object type = env.getVarType(var);
            OrderOperator.IOrder.OrderKind order = oc.getOrder();
            if (i == 0 && nkcfProvider != null && type != null) {
                nkcf = nkcfProvider.getNormalizedKeyComputerFactory(type, order == OrderOperator.IOrder.OrderKind.ASC);
            }
            IBinaryComparatorFactoryProvider bcfp = context.getBinaryComparatorFactoryProvider();
            comps[i] = bcfp.getBinaryComparatorFactory(type, oc.getOrder() == OrderOperator.IOrder.OrderKind.ASC);
            i++;
        }
        return new Triple<>(sortFields, comps, nkcf);
    }
}
