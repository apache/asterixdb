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
package org.apache.hyracks.algebricks.compiler.api;

import java.util.List;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.expressions.IConflictingTypeResolver;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IPartialAggregationTypeComputer;
import org.apache.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import org.apache.hyracks.algebricks.data.IBinaryHashFunctionFamilyProvider;
import org.apache.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import org.apache.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import org.apache.hyracks.algebricks.data.IPrinterFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public abstract class AbstractCompilerFactoryBuilder {

    protected List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRewrites;
    protected List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> physicalRewrites;
    protected ITypeTraitProvider typeTraitProvider;
    protected ISerializerDeserializerProvider serializerDeserializerProvider;
    protected IBinaryHashFunctionFactoryProvider hashFunctionFactoryProvider;
    protected IBinaryHashFunctionFamilyProvider hashFunctionFamilyProvider;
    protected IBinaryComparatorFactoryProvider comparatorFactoryProvider;
    protected IBinaryBooleanInspectorFactory binaryBooleanInspectorFactory;
    protected IBinaryIntegerInspectorFactory binaryIntegerInspectorFactory;
    protected IPrinterFactoryProvider printerProvider;
    protected IPredicateEvaluatorFactoryProvider predEvaluatorFactoryProvider;
    protected IExpressionRuntimeProvider expressionRuntimeProvider;
    protected IExpressionTypeComputer expressionTypeComputer;
    protected IMissableTypeComputer missableTypeComputer;
    protected IConflictingTypeResolver conflictingTypeResolver;
    protected IExpressionEvalSizeComputer expressionEvalSizeComputer;
    protected IMissingWriterFactory missingWriterFactory;
    protected INormalizedKeyComputerFactoryProvider normalizedKeyComputerFactoryProvider;
    protected IPartialAggregationTypeComputer partialAggregationTypeComputer;
    protected IMergeAggregationExpressionFactory mergeAggregationExpressionFactory;
    protected PhysicalOptimizationConfig physicalOptimizationConfig = new PhysicalOptimizationConfig();
    protected AlgebricksAbsolutePartitionConstraint clusterLocations;
    protected IWarningCollector warningCollector;
    protected long maxWarnings;

    public abstract ICompilerFactory create();

    public void setLogicalRewrites(List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRewrites) {
        this.logicalRewrites = logicalRewrites;
    }

    public void setPhysicalRewrites(List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> physicalRewrites) {
        this.physicalRewrites = physicalRewrites;
    }

    public void setTypeTraitProvider(ITypeTraitProvider typeTraitProvider) {
        this.typeTraitProvider = typeTraitProvider;
    }

    public ITypeTraitProvider getTypeTraitProvider() {
        return typeTraitProvider;
    }

    public void setSerializerDeserializerProvider(ISerializerDeserializerProvider serializerDeserializerProvider) {
        this.serializerDeserializerProvider = serializerDeserializerProvider;
    }

    public ISerializerDeserializerProvider getSerializerDeserializerProvider() {
        return serializerDeserializerProvider;
    }

    public void setHashFunctionFactoryProvider(IBinaryHashFunctionFactoryProvider hashFunctionFactoryProvider) {
        this.hashFunctionFactoryProvider = hashFunctionFactoryProvider;
    }

    public IBinaryHashFunctionFactoryProvider getHashFunctionFactoryProvider() {
        return hashFunctionFactoryProvider;
    }

    public void setHashFunctionFamilyProvider(IBinaryHashFunctionFamilyProvider hashFunctionFamilyProvider) {
        this.hashFunctionFamilyProvider = hashFunctionFamilyProvider;
    }

    public IBinaryHashFunctionFamilyProvider getHashFunctionFamilyProvider() {
        return hashFunctionFamilyProvider;
    }

    public void setComparatorFactoryProvider(IBinaryComparatorFactoryProvider comparatorFactoryProvider) {
        this.comparatorFactoryProvider = comparatorFactoryProvider;
    }

    public IBinaryComparatorFactoryProvider getComparatorFactoryProvider() {
        return comparatorFactoryProvider;
    }

    public void setPredicateEvaluatorFactoryProvider(IPredicateEvaluatorFactoryProvider predEvaluatorFactoryProvider) {
        this.predEvaluatorFactoryProvider = predEvaluatorFactoryProvider;
    }

    public IPredicateEvaluatorFactoryProvider getPredicateEvaluatorFactory() {
        return predEvaluatorFactoryProvider;
    }

    public void setBinaryBooleanInspectorFactory(IBinaryBooleanInspectorFactory binaryBooleanInspectorFactory) {
        this.binaryBooleanInspectorFactory = binaryBooleanInspectorFactory;
    }

    public IBinaryBooleanInspectorFactory getBinaryBooleanInspectorFactory() {
        return binaryBooleanInspectorFactory;
    }

    public void setBinaryIntegerInspectorFactory(IBinaryIntegerInspectorFactory binaryIntegerInspectorFactory) {
        this.binaryIntegerInspectorFactory = binaryIntegerInspectorFactory;
    }

    public IBinaryIntegerInspectorFactory getBinaryIntegerInspectorFactory() {
        return binaryIntegerInspectorFactory;
    }

    public void setPrinterProvider(IPrinterFactoryProvider printerProvider) {
        this.printerProvider = printerProvider;
    }

    public IPrinterFactoryProvider getPrinterProvider() {
        return printerProvider;
    }

    public void setExpressionRuntimeProvider(IExpressionRuntimeProvider expressionRuntimeProvider) {
        this.expressionRuntimeProvider = expressionRuntimeProvider;
    }

    public IExpressionRuntimeProvider getExpressionRuntimeProvider() {
        return expressionRuntimeProvider;
    }

    public void setExpressionTypeComputer(IExpressionTypeComputer expressionTypeComputer) {
        this.expressionTypeComputer = expressionTypeComputer;
    }

    public IExpressionTypeComputer getExpressionTypeComputer() {
        return expressionTypeComputer;
    }

    public void setClusterLocations(AlgebricksAbsolutePartitionConstraint clusterLocations) {
        this.clusterLocations = clusterLocations;
    }

    public AlgebricksPartitionConstraint getClusterLocations() {
        return clusterLocations;
    }

    public void setMissingWriterFactory(IMissingWriterFactory missingWriterFactory) {
        this.missingWriterFactory = missingWriterFactory;
    }

    public IMissingWriterFactory getMissingWriterFactory() {
        return missingWriterFactory;
    }

    public void setExpressionEvalSizeComputer(IExpressionEvalSizeComputer expressionEvalSizeComputer) {
        this.expressionEvalSizeComputer = expressionEvalSizeComputer;
    }

    public IExpressionEvalSizeComputer getExpressionEvalSizeComputer() {
        return expressionEvalSizeComputer;
    }

    public void setNormalizedKeyComputerFactoryProvider(
            INormalizedKeyComputerFactoryProvider normalizedKeyComputerFactoryProvider) {
        this.normalizedKeyComputerFactoryProvider = normalizedKeyComputerFactoryProvider;
    }

    public INormalizedKeyComputerFactoryProvider getNormalizedKeyComputerFactoryProvider() {
        return normalizedKeyComputerFactoryProvider;
    }

    public IPartialAggregationTypeComputer getPartialAggregationTypeComputer() {
        return partialAggregationTypeComputer;
    }

    public void setPartialAggregationTypeComputer(IPartialAggregationTypeComputer partialAggregationTypeComputer) {
        this.partialAggregationTypeComputer = partialAggregationTypeComputer;
    }

    public IMergeAggregationExpressionFactory getIMergeAggregationExpressionFactory() {
        return mergeAggregationExpressionFactory;
    }

    public void setIMergeAggregationExpressionFactory(
            IMergeAggregationExpressionFactory mergeAggregationExpressionFactory) {
        this.mergeAggregationExpressionFactory = mergeAggregationExpressionFactory;
    }

    public PhysicalOptimizationConfig getPhysicalOptimizationConfig() {
        return physicalOptimizationConfig;
    }

    public void setPhysicalOptimizationConfig(PhysicalOptimizationConfig physicalOptimizationConfig) {
        this.physicalOptimizationConfig = physicalOptimizationConfig;
    }

    public void setMissableTypeComputer(IMissableTypeComputer missableTypeComputer) {
        this.missableTypeComputer = missableTypeComputer;
    }

    public IMissableTypeComputer getMissableTypeComputer() {
        return missableTypeComputer;
    }

    public void setConflictingTypeResolver(IConflictingTypeResolver conflictingTypeResolver) {
        this.conflictingTypeResolver = conflictingTypeResolver;
    }

    public IConflictingTypeResolver getConflictingTypeResolver() {
        return conflictingTypeResolver;
    }

    public void setWarningCollector(IWarningCollector warningCollector) {
        this.warningCollector = warningCollector;
    }

    public IWarningCollector getWarningCollector() {
        return warningCollector;
    }

    public void setMaxWarnings(long maxWarnings) {
        this.maxWarnings = maxWarnings;
    }

    public long getMaxWarnings() {
        return maxWarnings;
    }
}
