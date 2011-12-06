/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import edu.uci.ics.hyracks.algebricks.core.algebra.data.IBinaryBooleanInspector;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IBinaryIntegerInspector;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.ITypeTraitProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ILogicalExpressionJobGen;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IPartialAggregationTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;

public abstract class AbstractCompilerFactoryBuilder {

    protected List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRewrites;
    protected List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> physicalRewrites;
    protected ITypeTraitProvider typeTraitProvider;
    protected ISerializerDeserializerProvider serializerDeserializerProvider;
    protected IBinaryHashFunctionFactoryProvider hashFunctionFactoryProvider;
    protected IBinaryComparatorFactoryProvider comparatorFactoryProvider;
    protected IBinaryBooleanInspector binaryBooleanInspector;
    protected IBinaryIntegerInspector binaryIntegerInspector;
    protected IPrinterFactoryProvider printerProvider;
    protected ILogicalExpressionJobGen exprJobGen;
    protected IExpressionTypeComputer expressionTypeComputer;
    protected INullableTypeComputer nullableTypeComputer;
    protected IExpressionEvalSizeComputer expressionEvalSizeComputer;
    protected INullWriterFactory nullWriterFactory;
    protected INormalizedKeyComputerFactoryProvider normalizedKeyComputerFactoryProvider;
    protected IPartialAggregationTypeComputer partialAggregationTypeComputer;
    protected IMergeAggregationExpressionFactory mergeAggregationExpressionFactory;
    protected PhysicalOptimizationConfig physicalOptimizationConfig = new PhysicalOptimizationConfig();
    protected AlgebricksPartitionConstraint clusterLocations;
    protected int frameSize = -1;

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

    public void setComparatorFactoryProvider(IBinaryComparatorFactoryProvider comparatorFactoryProvider) {
        this.comparatorFactoryProvider = comparatorFactoryProvider;
    }

    public IBinaryComparatorFactoryProvider getComparatorFactoryProvider() {
        return comparatorFactoryProvider;
    }

    public void setBinaryBooleanInspector(IBinaryBooleanInspector binaryBooleanInspector) {
        this.binaryBooleanInspector = binaryBooleanInspector;
    }

    public IBinaryBooleanInspector getBinaryBooleanInspector() {
        return binaryBooleanInspector;
    }

    public void setBinaryIntegerInspector(IBinaryIntegerInspector binaryIntegerInspector) {
        this.binaryIntegerInspector = binaryIntegerInspector;
    }

    public IBinaryIntegerInspector getBinaryIntegerInspector() {
        return binaryIntegerInspector;
    }

    public void setPrinterProvider(IPrinterFactoryProvider printerProvider) {
        this.printerProvider = printerProvider;
    }

    public IPrinterFactoryProvider getPrinterProvider() {
        return printerProvider;
    }

    public void setExprJobGen(ILogicalExpressionJobGen exprJobGen) {
        this.exprJobGen = exprJobGen;
    }

    public ILogicalExpressionJobGen getExprJobGen() {
        return exprJobGen;
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

    public void setFrameSize(int frameSize) {
        this.frameSize = frameSize;
    }

    public int getFrameSize() {
        return frameSize;
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
