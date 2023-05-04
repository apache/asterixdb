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

package org.apache.asterix.optimizer.cost;

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.optimizer.rules.cbo.JoinNode;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;

public class CostMethods implements ICostMethods {

    protected IOptimizationContext optCtx;
    protected PhysicalOptimizationConfig physOptConfig;
    protected long blockSize;
    protected long DOP;
    protected double maxMemorySize;

    public CostMethods(IOptimizationContext context) {
        optCtx = context;
        physOptConfig = context.getPhysicalOptimizationConfig();
        blockSize = getBufferCachePageSize();
        DOP = getDOP();
        maxMemorySize = getMaxMemorySize();
    }

    private long getBufferCacheSize() {
        MetadataProvider metadataProvider = (MetadataProvider) optCtx.getMetadataProvider();
        return metadataProvider.getStorageProperties().getBufferCacheSize();
    }

    private long getBufferCachePageSize() {
        MetadataProvider metadataProvider = (MetadataProvider) optCtx.getMetadataProvider();
        return metadataProvider.getStorageProperties().getBufferCachePageSize();
    }

    public long getDOP() {
        return optCtx.getComputationNodeDomain().cardinality();
    }

    public double getMaxMemorySize() {
        return physOptConfig.getMaxFramesForJoin() * physOptConfig.getFrameSize();
    }

    // These cost methods are very simple and rudimentary for now. These can be improved by asterixdb developers as needed.
    public Cost costFullScan(JoinNode jn) {
        return new Cost(jn.computeJoinCardinality());
    }

    public Cost costIndexScan(JoinNode jn, double indexSel) {
        return new Cost(jn.computeJoinCardinality());
    }

    public Cost costIndexDataScan(JoinNode jn, double indexSel) {
        return new Cost(jn.computeJoinCardinality());
    }

    public Cost costHashJoin(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        JoinNode rightJn = jn.getRightJn();
        return new Cost(leftJn.computeJoinCardinality() + rightJn.computeJoinCardinality());
    }

    public Cost computeHJProbeExchangeCost(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        return new Cost(leftJn.computeJoinCardinality());
    }

    public Cost computeHJBuildExchangeCost(JoinNode jn) {
        JoinNode rightJn = jn.getRightJn();
        return new Cost(rightJn.computeJoinCardinality());
    }

    public Cost costBroadcastHashJoin(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        JoinNode rightJn = jn.getRightJn();
        return new Cost(leftJn.computeJoinCardinality() + DOP * rightJn.computeJoinCardinality());
    }

    public Cost computeBHJBuildExchangeCost(JoinNode jn) {
        JoinNode rightJn = jn.getRightJn();
        return new Cost(DOP * rightJn.computeJoinCardinality());
    }

    public Cost costIndexNLJoin(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        JoinNode rightJn = jn.getRightJn();
        return new Cost(leftJn.computeJoinCardinality());
    }

    public Cost computeNLJOuterExchangeCost(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        return new Cost(DOP * leftJn.computeJoinCardinality());
    }

    public Cost costCartesianProductJoin(JoinNode jn) {
        JoinNode leftJn = jn.getLeftJn();
        JoinNode rightJn = jn.getRightJn();
        return new Cost(leftJn.computeJoinCardinality() * rightJn.computeJoinCardinality());
    }

    public Cost computeCPRightExchangeCost(JoinNode jn) {
        JoinNode rightJn = jn.getRightJn();
        return new Cost(DOP * rightJn.computeJoinCardinality());
    }
}
