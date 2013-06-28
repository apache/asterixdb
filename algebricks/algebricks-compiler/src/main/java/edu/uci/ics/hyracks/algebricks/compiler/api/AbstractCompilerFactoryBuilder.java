/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.compiler.api;

import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IPartialAggregationTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFamilyProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import edu.uci.ics.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.algebricks.data.ITypeTraitProvider;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;

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
    protected INullableTypeComputer nullableTypeComputer;
    protected IExpressionEvalSizeComputer expressionEvalSizeComputer;
    protected INullWriterFactory nullWriterFactory;
    protected INormalizedKeyComputerFactoryProvider normalizedKeyComputerFactoryProvider;
    protected IPartialAggregationTypeComputer partialAggregationTypeComputer;
    protected IMergeAggregationExpressionFactory mergeAggregationExpressionFactory;
    protected PhysicalOptimizationConfig physicalOptimizationConfig = new PhysicalOptimizationConfig();
    protected AlgebricksPartitionConstraint clusterLocations;

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

    public void setClusterLocations(AlgebricksPartitionConstraint clusterLocations) {
        this.clusterLocations = clusterLocations;
    }

    public AlgebricksPartitionConstraint getClusterLocations() {
        return clusterLocations;
    }

    public void setNullWriterFactory(INullWriterFactory nullWriterFactory) {
        this.nullWriterFactory = nullWriterFactory;
    }

    public INullWriterFactory getNullWriterFactory() {
        return nullWriterFactory;
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

    public void setNullableTypeComputer(INullableTypeComputer nullableTypeComputer) {
        this.nullableTypeComputer = nullableTypeComputer;
    }

    public INullableTypeComputer getNullableTypeComputer() {
        return nullableTypeComputer;
    }

}
