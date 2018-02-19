/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.app.resource;

import static org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag.GROUP;
import static org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag.INNERJOIN;
import static org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag.LEFTOUTERJOIN;
import static org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag.ORDER;
import static org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode.LOCAL;
import static org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode.PARTITIONED;
import static org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode.UNPARTITIONED;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.asterix.utils.ResourceUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistributeResultOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ExternalGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HashPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.PreclusteredGroupByPOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.junit.Assert;
import org.junit.Test;

public class PlanStagesGeneratorTest {

    private static final Set<LogicalOperatorTag> BLOCKING_OPERATORS =
            new HashSet<>(Arrays.asList(INNERJOIN, LEFTOUTERJOIN, ORDER));
    private static final long MEMORY_BUDGET = 33554432L;
    private static final int FRAME_SIZE = 32768;
    private static final int FRAME_LIMIT = (int) (MEMORY_BUDGET / FRAME_SIZE);
    private static final int PARALLELISM = 10;
    private static final long MAX_BUFFER_PER_CONNECTION = 1L;

    @Test
    public void noBlockingPlan() throws AlgebricksException {
        EmptyTupleSourceOperator ets = new EmptyTupleSourceOperator();
        ets.setExecutionMode(UNPARTITIONED);

        AssignOperator assignOperator = new AssignOperator(Collections.emptyList(), null);
        assignOperator.setExecutionMode(UNPARTITIONED);
        assignOperator.getInputs().add(new MutableObject<>(ets));

        ExchangeOperator exchange = new ExchangeOperator();
        exchange.setExecutionMode(UNPARTITIONED);
        exchange.setPhysicalOperator(new OneToOneExchangePOperator());
        exchange.getInputs().add(new MutableObject<>(assignOperator));

        DistributeResultOperator resultOperator = new DistributeResultOperator(null, null);
        resultOperator.setExecutionMode(UNPARTITIONED);
        resultOperator.getInputs().add(new MutableObject<>(exchange));
        ALogicalPlanImpl plan = new ALogicalPlanImpl(Collections.singletonList(new MutableObject(resultOperator)));

        List<PlanStage> stages = ResourceUtils.getStages(plan);
        // ensure a single stage plan
        final int expectedStages = 1;
        Assert.assertEquals(expectedStages, stages.size());
        validateStages(stages, resultOperator, exchange, ets, assignOperator);
        // frame size for every operator
        final long expectedMemory = stages.get(0).getOperators().size() * FRAME_SIZE;
        assertRequiredMemory(stages, expectedMemory);
    }

    @Test
    public void testNonBlockingGroupByOrderBy() throws AlgebricksException {
        EmptyTupleSourceOperator ets = new EmptyTupleSourceOperator();
        ets.setExecutionMode(PARTITIONED);

        DataSourceScanOperator scanOperator = new DataSourceScanOperator(Collections.emptyList(), null);
        scanOperator.setExecutionMode(PARTITIONED);
        scanOperator.getInputs().add(new MutableObject<>(ets));

        ExchangeOperator exchange = new ExchangeOperator();
        exchange.setExecutionMode(PARTITIONED);
        exchange.setPhysicalOperator(new OneToOneExchangePOperator());
        exchange.getInputs().add(new MutableObject<>(scanOperator));

        GroupByOperator groupByOperator = new GroupByOperator();
        groupByOperator.setExecutionMode(PARTITIONED);
        groupByOperator
                .setPhysicalOperator(new PreclusteredGroupByPOperator(Collections.emptyList(), true, FRAME_LIMIT));
        groupByOperator.getInputs().add(new MutableObject<>(exchange));

        OrderOperator orderOperator = new OrderOperator();
        orderOperator.setExecutionMode(PARTITIONED);
        orderOperator.getInputs().add(new MutableObject<>(groupByOperator));

        DistributeResultOperator resultOperator = new DistributeResultOperator(null, null);
        resultOperator.setExecutionMode(PARTITIONED);
        resultOperator.getInputs().add(new MutableObject<>(orderOperator));
        ALogicalPlanImpl plan = new ALogicalPlanImpl(Collections.singletonList(new MutableObject(resultOperator)));

        final List<PlanStage> stages = ResourceUtils.getStages(plan);
        validateStages(stages, ets, exchange, groupByOperator, orderOperator, resultOperator);
        // ensure 3 stage (root to order, order to group by, group by to ets)
        final int expectedStages = 2;
        Assert.assertEquals(expectedStages, stages.size());

        // dominating stage should have orderBy, orderBy's input (groupby), groupby's input (exchange),
        // exchange's input (scanOperator), and scanOperator's input (ets)
        long orderOperatorRequiredMemory = FRAME_LIMIT * FRAME_SIZE * PARALLELISM;
        long groupByOperatorRequiredMemory = FRAME_LIMIT * FRAME_SIZE * PARALLELISM;
        long exchangeRequiredMemory = PARALLELISM * FRAME_SIZE;
        long scanOperatorRequiredMemory = PARALLELISM * FRAME_SIZE;
        long etsRequiredMemory = FRAME_SIZE * PARALLELISM;

        final long expectedMemory = orderOperatorRequiredMemory + groupByOperatorRequiredMemory + exchangeRequiredMemory
                + scanOperatorRequiredMemory + etsRequiredMemory;
        assertRequiredMemory(stages, expectedMemory);
    }

    @Test
    public void testJoinGroupby() throws AlgebricksException {
        EmptyTupleSourceOperator ets1 = new EmptyTupleSourceOperator();
        ets1.setExecutionMode(PARTITIONED);

        DataSourceScanOperator scanOperator1 = new DataSourceScanOperator(Collections.emptyList(), null);
        scanOperator1.setExecutionMode(PARTITIONED);
        scanOperator1.getInputs().add(new MutableObject<>(ets1));

        EmptyTupleSourceOperator ets2 = new EmptyTupleSourceOperator();
        ets1.setExecutionMode(PARTITIONED);

        DataSourceScanOperator scanOperator2 = new DataSourceScanOperator(Collections.emptyList(), null);
        scanOperator2.setExecutionMode(PARTITIONED);
        scanOperator2.getInputs().add(new MutableObject<>(ets2));

        InnerJoinOperator firstJoin = new InnerJoinOperator(new MutableObject<>(ConstantExpression.TRUE));
        firstJoin.setExecutionMode(PARTITIONED);
        firstJoin.getInputs().add(new MutableObject<>(scanOperator1));
        firstJoin.getInputs().add(new MutableObject<>(scanOperator2));

        ExchangeOperator exchangeOperator1 = new ExchangeOperator();
        exchangeOperator1.setExecutionMode(PARTITIONED);
        exchangeOperator1.setPhysicalOperator(new HashPartitionExchangePOperator(Collections.emptyList(), null));
        exchangeOperator1.getInputs().add(new MutableObject<>(firstJoin));

        EmptyTupleSourceOperator ets3 = new EmptyTupleSourceOperator();
        ets1.setExecutionMode(PARTITIONED);

        GroupByOperator groupByOperator = new GroupByOperator();
        groupByOperator
                .setPhysicalOperator(new ExternalGroupByPOperator(Collections.emptyList(), FRAME_LIMIT, FRAME_LIMIT));
        groupByOperator.setExecutionMode(LOCAL);
        groupByOperator.getInputs().add(new MutableObject<>(ets3));

        ExchangeOperator exchangeOperator2 = new ExchangeOperator();
        exchangeOperator2.setExecutionMode(PARTITIONED);
        exchangeOperator2.setPhysicalOperator(new HashPartitionExchangePOperator(Collections.emptyList(), null));
        exchangeOperator2.getInputs().add(new MutableObject<>(groupByOperator));

        LeftOuterJoinOperator secondJoin = new LeftOuterJoinOperator(new MutableObject<>(ConstantExpression.TRUE));
        secondJoin.setExecutionMode(PARTITIONED);
        secondJoin.getInputs().add(new MutableObject<>(exchangeOperator1));
        secondJoin.getInputs().add(new MutableObject<>(exchangeOperator2));

        DistributeResultOperator resultOperator = new DistributeResultOperator(null, null);
        resultOperator.setExecutionMode(PARTITIONED);
        resultOperator.getInputs().add(new MutableObject<>(secondJoin));
        ALogicalPlanImpl plan = new ALogicalPlanImpl(Collections.singletonList(new MutableObject(resultOperator)));

        List<PlanStage> stages = ResourceUtils.getStages(plan);
        final int expectedStages = 4;
        Assert.assertEquals(expectedStages, stages.size());
        validateStages(stages, ets1, scanOperator1, ets2, scanOperator2, firstJoin, exchangeOperator1, ets3,
                groupByOperator, exchangeOperator2, secondJoin, resultOperator);

        // dominating stage should have the following operators:
        // resultOperator, its input (secondJoin), secondJoin's first input (exchangeOperator1), exchangeOperator1's
        // input (firstJoin), firstJoin's first input (scanOperator1), and scanOperator1's input (ets1)
        long resultOperatorRequiredMemory = FRAME_SIZE * PARALLELISM;
        long secondJoinRequiredMemory = FRAME_LIMIT * FRAME_SIZE * PARALLELISM;
        long exchangeOperator1RequiredMemory = 2 * MAX_BUFFER_PER_CONNECTION * PARALLELISM * PARALLELISM * FRAME_SIZE;
        long firstJoinRequiredMemory = FRAME_LIMIT * FRAME_SIZE * PARALLELISM;
        long scanOperator1RequiredMemory = FRAME_SIZE * PARALLELISM;
        long ets1RequiredMemory = FRAME_SIZE * PARALLELISM;

        long expectedMemory = resultOperatorRequiredMemory + secondJoinRequiredMemory + exchangeOperator1RequiredMemory
                + firstJoinRequiredMemory + scanOperator1RequiredMemory + ets1RequiredMemory;
        assertRequiredMemory(stages, expectedMemory);
    }

    @Test
    public void testReplicateSortJoin() throws AlgebricksException {
        EmptyTupleSourceOperator ets = new EmptyTupleSourceOperator();
        ets.setExecutionMode(PARTITIONED);

        DataSourceScanOperator scanOperator = new DataSourceScanOperator(Collections.emptyList(), null);
        scanOperator.setExecutionMode(PARTITIONED);
        scanOperator.getInputs().add(new MutableObject<>(ets));

        ReplicateOperator replicateOperator = new ReplicateOperator(2);
        replicateOperator.setExecutionMode(PARTITIONED);
        replicateOperator.getInputs().add(new MutableObject<>(scanOperator));

        OrderOperator order1 = new OrderOperator();
        order1.setExecutionMode(PARTITIONED);
        order1.setPhysicalOperator(new OneToOneExchangePOperator());
        order1.getInputs().add(new MutableObject<>(replicateOperator));

        OrderOperator order2 = new OrderOperator();
        order2.setExecutionMode(PARTITIONED);
        order2.setPhysicalOperator(new OneToOneExchangePOperator());
        order2.getInputs().add(new MutableObject<>(replicateOperator));

        LeftOuterJoinOperator secondJoin = new LeftOuterJoinOperator(new MutableObject<>(ConstantExpression.TRUE));
        secondJoin.setExecutionMode(PARTITIONED);
        secondJoin.getInputs().add(new MutableObject<>(order1));
        secondJoin.getInputs().add(new MutableObject<>(order2));

        DistributeResultOperator resultOperator = new DistributeResultOperator(null, null);
        resultOperator.setExecutionMode(PARTITIONED);
        resultOperator.getInputs().add(new MutableObject<>(secondJoin));
        ALogicalPlanImpl plan = new ALogicalPlanImpl(Collections.singletonList(new MutableObject(resultOperator)));

        List<PlanStage> stages = ResourceUtils.getStages(plan);
        final int expectedStages = 3;
        Assert.assertEquals(expectedStages, stages.size());
        validateStages(stages);

        // dominating stage should have the following operators:
        // secondJoin, secondJoin's second input (order2), order2's input (replicate),
        // replicate's input (scanOperator), scanOperator's input (ets)
        long secondJoinRequiredMemory = FRAME_LIMIT * FRAME_SIZE * PARALLELISM;
        long order2RequiredMemory = FRAME_LIMIT * FRAME_SIZE * PARALLELISM;
        long replicateOperatorRequiredMemory = FRAME_SIZE * PARALLELISM;
        long scanOperator1RequiredMemory = FRAME_SIZE * PARALLELISM;
        long etsRequiredMemory = FRAME_SIZE * PARALLELISM;
        long expectedMemory = secondJoinRequiredMemory + order2RequiredMemory + replicateOperatorRequiredMemory
                + scanOperator1RequiredMemory + etsRequiredMemory;
        assertRequiredMemory(stages, expectedMemory);
    }

    private void validateStages(List<PlanStage> stages, ILogicalOperator... operators) {
        // ensure all operators appear
        Stream.of(operators).forEach(op -> ensureOperatorExists(stages, op));
        // ensure the correct count
        for (PlanStage stage : stages) {
            stage.getOperators().forEach(op -> validateOperatorStages(stages, op));
        }
    }

    private void ensureOperatorExists(List<PlanStage> stages, ILogicalOperator operator) {
        final long actual = stages.stream().map(PlanStage::getOperators).filter(op -> op.contains(operator)).count();
        Assert.assertTrue(actual > 0);
    }

    private void validateOperatorStages(List<PlanStage> stages, ILogicalOperator operator) {
        if (stages.size() == 1) {
            return;
        }
        long expectedAppearances = BLOCKING_OPERATORS.contains(operator.getOperatorTag()) ? 2 : 1;
        if (operator.getOperatorTag() == GROUP) {
            GroupByOperator groupByOperator = (GroupByOperator) operator;
            if (groupByOperator.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.EXTERNAL_GROUP_BY
                    || groupByOperator.getPhysicalOperator().getOperatorTag() == PhysicalOperatorTag.SORT_GROUP_BY) {
                expectedAppearances = 2;
            }
        }
        final long actual = stages.stream().map(PlanStage::getOperators).filter(op -> op.contains(operator)).count();
        Assert.assertEquals(expectedAppearances, actual);
    }

    private void assertRequiredMemory(List<PlanStage> stages, long expectedMemory) {
        final IClusterCapacity clusterCapacity = ResourceUtils.getStageBasedRequiredCapacity(stages, PARALLELISM,
                FRAME_LIMIT, FRAME_LIMIT, FRAME_LIMIT, FRAME_LIMIT, FRAME_SIZE);
        Assert.assertEquals(clusterCapacity.getAggregatedMemoryByteSize(), expectedMemory);
    }
}
