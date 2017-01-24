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

import java.util.Collections;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HashPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.api.job.resource.ClusterCapacity;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.junit.Assert;
import org.junit.Test;

public class RequiredCapacityVisitorTest {

    private static final long MEMORY_BUDGET = 33554432L;
    private static final int FRAME_SIZE = 32768;
    private static final int FRAME_LIMIT = (int) (MEMORY_BUDGET / FRAME_SIZE);
    private static final int PARALLELISM = 10;

    @Test
    public void testParallelGroupBy() throws AlgebricksException {
        IClusterCapacity clusterCapacity = new ClusterCapacity();
        RequiredCapacityVisitor visitor = makeComputationCapacityVisitor(PARALLELISM, clusterCapacity);

        // Constructs a parallel group-by query plan.
        GroupByOperator globalGby = makeGroupByOperator(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        ExchangeOperator exchange = new ExchangeOperator();
        exchange.setPhysicalOperator(new HashPartitionExchangePOperator(Collections.emptyList(), null));
        GroupByOperator localGby = makeGroupByOperator(AbstractLogicalOperator.ExecutionMode.LOCAL);
        globalGby.getInputs().add(new MutableObject<>(exchange));
        exchange.getInputs().add(new MutableObject<>(localGby));

        // Verifies the calculated cluster capacity requirement for the test quer plan.
        globalGby.accept(visitor, null);
        Assert.assertTrue(clusterCapacity.getAggregatedCores() == PARALLELISM);
        Assert.assertTrue(clusterCapacity.getAggregatedMemoryByteSize() == 2 * MEMORY_BUDGET * PARALLELISM
                + 2 * FRAME_SIZE * PARALLELISM * PARALLELISM);
    }

    @Test
    public void testUnPartitionedGroupBy() throws AlgebricksException {
        IClusterCapacity clusterCapacity = new ClusterCapacity();
        RequiredCapacityVisitor visitor = makeComputationCapacityVisitor(PARALLELISM, clusterCapacity);

        // Constructs a parallel group-by query plan.
        GroupByOperator globalGby = makeGroupByOperator(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        ExchangeOperator exchange = new ExchangeOperator();
        exchange.setPhysicalOperator(new OneToOneExchangePOperator());
        exchange.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        GroupByOperator localGby = makeGroupByOperator(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        globalGby.getInputs().add(new MutableObject<>(exchange));
        exchange.getInputs().add(new MutableObject<>(localGby));

        // Verifies the calculated cluster capacity requirement for the test quer plan.
        globalGby.accept(visitor, null);
        Assert.assertTrue(clusterCapacity.getAggregatedCores() == 1);
        Assert.assertTrue(clusterCapacity.getAggregatedMemoryByteSize() == 2 * MEMORY_BUDGET + FRAME_SIZE);
    }

    @Test
    public void testParallelJoin() throws AlgebricksException {
        IClusterCapacity clusterCapacity = new ClusterCapacity();
        RequiredCapacityVisitor visitor = makeComputationCapacityVisitor(PARALLELISM, clusterCapacity);

        // Constructs a join query plan.
        InnerJoinOperator join = makeJoinOperator(AbstractLogicalOperator.ExecutionMode.PARTITIONED);

        // Left child plan of the join.
        ExchangeOperator leftChildExchange = new ExchangeOperator();
        leftChildExchange.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        leftChildExchange.setPhysicalOperator(new HashPartitionExchangePOperator(Collections.emptyList(), null));
        InnerJoinOperator leftChild = makeJoinOperator(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        join.getInputs().add(new MutableObject<>(leftChildExchange));
        leftChildExchange.getInputs().add(new MutableObject<>(leftChild));
        EmptyTupleSourceOperator ets = new EmptyTupleSourceOperator();
        ets.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        leftChild.getInputs().add(new MutableObject<>(ets));
        leftChild.getInputs().add(new MutableObject<>(ets));

        // Right child plan of the join.
        ExchangeOperator rightChildExchange = new ExchangeOperator();
        rightChildExchange.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        rightChildExchange.setPhysicalOperator(new HashPartitionExchangePOperator(Collections.emptyList(), null));
        GroupByOperator rightChild = makeGroupByOperator(AbstractLogicalOperator.ExecutionMode.LOCAL);
        join.getInputs().add(new MutableObject<>(rightChildExchange));
        rightChildExchange.getInputs().add(new MutableObject<>(rightChild));
        rightChild.getInputs().add(new MutableObject<>(ets));

        // Verifies the calculated cluster capacity requirement for the test quer plan.
        join.accept(visitor, null);
        Assert.assertTrue(clusterCapacity.getAggregatedCores() == PARALLELISM);
        Assert.assertTrue(clusterCapacity.getAggregatedMemoryByteSize() == 3 * MEMORY_BUDGET * PARALLELISM
                + 2 * 2L * PARALLELISM * PARALLELISM * FRAME_SIZE + 3 * FRAME_SIZE * PARALLELISM);
    }

    @Test
    public void testUnPartitionedJoin() throws AlgebricksException {
        IClusterCapacity clusterCapacity = new ClusterCapacity();
        RequiredCapacityVisitor visitor = makeComputationCapacityVisitor(PARALLELISM, clusterCapacity);

        // Constructs a join query plan.
        InnerJoinOperator join = makeJoinOperator(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);

        // Left child plan of the join.
        ExchangeOperator leftChildExchange = new ExchangeOperator();
        leftChildExchange.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        leftChildExchange.setPhysicalOperator(new OneToOneExchangePOperator());
        InnerJoinOperator leftChild = makeJoinOperator(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        join.getInputs().add(new MutableObject<>(leftChildExchange));
        leftChildExchange.getInputs().add(new MutableObject<>(leftChild));
        EmptyTupleSourceOperator ets = new EmptyTupleSourceOperator();
        ets.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        leftChild.getInputs().add(new MutableObject<>(ets));
        leftChild.getInputs().add(new MutableObject<>(ets));

        // Right child plan of the join.
        ExchangeOperator rightChildExchange = new ExchangeOperator();
        rightChildExchange.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        rightChildExchange.setPhysicalOperator(new OneToOneExchangePOperator());
        GroupByOperator rightChild = makeGroupByOperator(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        join.getInputs().add(new MutableObject<>(rightChildExchange));
        rightChildExchange.getInputs().add(new MutableObject<>(rightChild));
        rightChild.getInputs().add(new MutableObject<>(ets));

        // Verifies the calculated cluster capacity requirement for the test quer plan.
        join.accept(visitor, null);
        Assert.assertTrue(clusterCapacity.getAggregatedCores() == 1);
        Assert.assertTrue(clusterCapacity.getAggregatedMemoryByteSize() == 3 * MEMORY_BUDGET + 5L * FRAME_SIZE);
    }

    private RequiredCapacityVisitor makeComputationCapacityVisitor(int numComputationPartitions,
            IClusterCapacity clusterCapacity) {
        return new RequiredCapacityVisitor(numComputationPartitions, FRAME_LIMIT, FRAME_LIMIT, FRAME_LIMIT, FRAME_SIZE,
                clusterCapacity);
    }

    private GroupByOperator makeGroupByOperator(AbstractLogicalOperator.ExecutionMode exeMode) {
        GroupByOperator groupByOperator = new GroupByOperator();
        groupByOperator.setExecutionMode(exeMode);
        return groupByOperator;
    }

    private InnerJoinOperator makeJoinOperator(AbstractLogicalOperator.ExecutionMode exeMode) {
        InnerJoinOperator joinOperator = new InnerJoinOperator(new MutableObject<>(ConstantExpression.TRUE));
        joinOperator.setExecutionMode(exeMode);
        return joinOperator;
    }
}
