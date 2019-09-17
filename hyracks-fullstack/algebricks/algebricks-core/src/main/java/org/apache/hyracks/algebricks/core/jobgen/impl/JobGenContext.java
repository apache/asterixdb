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
package org.apache.hyracks.algebricks.core.jobgen.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IPartialAggregationTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
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

public class JobGenContext {
    private final IOperatorSchema outerFlowSchema;
    private final Map<ILogicalOperator, IOperatorSchema> schemaMap = new HashMap<ILogicalOperator, IOperatorSchema>();
    private final ISerializerDeserializerProvider serializerDeserializerProvider;
    private final IBinaryHashFunctionFactoryProvider hashFunctionFactoryProvider;
    private final IBinaryHashFunctionFamilyProvider hashFunctionFamilyProvider;
    private final IBinaryComparatorFactoryProvider comparatorFactoryProvider;
    private final IPrinterFactoryProvider printerFactoryProvider;
    private final ITypeTraitProvider typeTraitProvider;
    private final IMetadataProvider<?, ?> metadataProvider;
    private final IMissingWriterFactory nonMatchWriterFactory;
    private final INormalizedKeyComputerFactoryProvider normalizedKeyComputerFactoryProvider;
    private final Object appContext;
    private final IBinaryBooleanInspectorFactory booleanInspectorFactory;
    private final IBinaryIntegerInspectorFactory integerInspectorFactory;
    private final IExpressionRuntimeProvider expressionRuntimeProvider;
    private final IExpressionTypeComputer expressionTypeComputer;
    private final IExpressionEvalSizeComputer expressionEvalSizeComputer;
    private final IPartialAggregationTypeComputer partialAggregationTypeComputer;
    private final IPredicateEvaluatorFactoryProvider predEvaluatorFactoryProvider;
    private final int frameSize;
    private AlgebricksAbsolutePartitionConstraint clusterLocations;
    private int varCounter;
    private final ITypingContext typingContext;
    private final long maxWarnings;

    public JobGenContext(IOperatorSchema outerFlowSchema, IMetadataProvider<?, ?> metadataProvider, Object appContext,
            ISerializerDeserializerProvider serializerDeserializerProvider,
            IBinaryHashFunctionFactoryProvider hashFunctionFactoryProvider,
            IBinaryHashFunctionFamilyProvider hashFunctionFamilyProvider,
            IBinaryComparatorFactoryProvider comparatorFactoryProvider, ITypeTraitProvider typeTraitProvider,
            IBinaryBooleanInspectorFactory booleanInspectorFactory,
            IBinaryIntegerInspectorFactory integerInspectorFactory, IPrinterFactoryProvider printerFactoryProvider,
            IMissingWriterFactory nullWriterFactory,
            INormalizedKeyComputerFactoryProvider normalizedKeyComputerFactoryProvider,
            IExpressionRuntimeProvider expressionRuntimeProvider, IExpressionTypeComputer expressionTypeComputer,
            ITypingContext typingContext, IExpressionEvalSizeComputer expressionEvalSizeComputer,
            IPartialAggregationTypeComputer partialAggregationTypeComputer,
            IPredicateEvaluatorFactoryProvider predEvaluatorFactoryProvider, int frameSize,
            AlgebricksAbsolutePartitionConstraint clusterLocations, long maxWarnings) {
        this.outerFlowSchema = outerFlowSchema;
        this.metadataProvider = metadataProvider;
        this.appContext = appContext;
        this.serializerDeserializerProvider = serializerDeserializerProvider;
        this.hashFunctionFactoryProvider = hashFunctionFactoryProvider;
        this.hashFunctionFamilyProvider = hashFunctionFamilyProvider;
        this.comparatorFactoryProvider = comparatorFactoryProvider;
        this.typeTraitProvider = typeTraitProvider;
        this.booleanInspectorFactory = booleanInspectorFactory;
        this.integerInspectorFactory = integerInspectorFactory;
        this.printerFactoryProvider = printerFactoryProvider;
        this.clusterLocations = clusterLocations;
        this.normalizedKeyComputerFactoryProvider = normalizedKeyComputerFactoryProvider;
        this.nonMatchWriterFactory = nullWriterFactory;
        this.expressionRuntimeProvider = expressionRuntimeProvider;
        this.expressionTypeComputer = expressionTypeComputer;
        this.typingContext = typingContext;
        this.expressionEvalSizeComputer = expressionEvalSizeComputer;
        this.partialAggregationTypeComputer = partialAggregationTypeComputer;
        this.predEvaluatorFactoryProvider = predEvaluatorFactoryProvider;
        this.frameSize = frameSize;
        this.varCounter = 0;
        this.maxWarnings = maxWarnings;
    }

    public IOperatorSchema getOuterFlowSchema() {
        return outerFlowSchema;
    }

    public AlgebricksAbsolutePartitionConstraint getClusterLocations() {
        return clusterLocations;
    }

    public IMetadataProvider<?, ?> getMetadataProvider() {
        return metadataProvider;
    }

    public Object getAppContext() {
        return appContext;
    }

    public ISerializerDeserializerProvider getSerializerDeserializerProvider() {
        return serializerDeserializerProvider;
    }

    public IBinaryHashFunctionFactoryProvider getBinaryHashFunctionFactoryProvider() {
        return hashFunctionFactoryProvider;
    }

    public IBinaryHashFunctionFamilyProvider getBinaryHashFunctionFamilyProvider() {
        return hashFunctionFamilyProvider;
    }

    public IBinaryComparatorFactoryProvider getBinaryComparatorFactoryProvider() {
        return comparatorFactoryProvider;
    }

    public ITypeTraitProvider getTypeTraitProvider() {
        return typeTraitProvider;
    }

    public IBinaryBooleanInspectorFactory getBinaryBooleanInspectorFactory() {
        return booleanInspectorFactory;
    }

    public IBinaryIntegerInspectorFactory getBinaryIntegerInspectorFactory() {
        return integerInspectorFactory;
    }

    public IPrinterFactoryProvider getPrinterFactoryProvider() {
        return printerFactoryProvider;
    }

    public IPredicateEvaluatorFactoryProvider getPredicateEvaluatorFactoryProvider() {
        return predEvaluatorFactoryProvider;
    }

    public IExpressionRuntimeProvider getExpressionRuntimeProvider() {
        return expressionRuntimeProvider;
    }

    public IOperatorSchema getSchema(ILogicalOperator op) {
        return schemaMap.get(op);
    }

    public void putSchema(ILogicalOperator op, IOperatorSchema schema) {
        schemaMap.put(op, schema);
    }

    public LogicalVariable createNewVar() {
        varCounter++;
        LogicalVariable var = new LogicalVariable(-varCounter);
        return var;
    }

    public Object getType(ILogicalExpression expr, IVariableTypeEnvironment env) throws AlgebricksException {
        return expressionTypeComputer.getType(expr, typingContext.getMetadataProvider(), env);
    }

    public IMissingWriterFactory getMissingWriterFactory() {
        return nonMatchWriterFactory;
    }

    public INormalizedKeyComputerFactoryProvider getNormalizedKeyComputerFactoryProvider() {
        return normalizedKeyComputerFactoryProvider;
    }

    public IExpressionEvalSizeComputer getExpressionEvalSizeComputer() {
        return expressionEvalSizeComputer;
    }

    public int getFrameSize() {
        return frameSize;
    }

    public IPartialAggregationTypeComputer getPartialAggregationTypeComputer() {
        return partialAggregationTypeComputer;
    }

    public IVariableTypeEnvironment getTypeEnvironment(ILogicalOperator op) {
        return typingContext.getOutputTypeEnvironment(op);
    }

    public long getMaxWarnings() {
        return maxWarnings;
    }
}
