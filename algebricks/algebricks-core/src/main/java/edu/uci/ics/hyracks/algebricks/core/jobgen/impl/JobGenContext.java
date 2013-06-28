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
package edu.uci.ics.hyracks.algebricks.core.jobgen.impl;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IPartialAggregationTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
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
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;

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
	private final INullWriterFactory nullWriterFactory;
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
	private AlgebricksPartitionConstraint clusterLocations;
	private int varCounter;
	private final ITypingContext typingContext;

	public JobGenContext(
			IOperatorSchema outerFlowSchema,
			IMetadataProvider<?, ?> metadataProvider,
			Object appContext,
			ISerializerDeserializerProvider serializerDeserializerProvider,
			IBinaryHashFunctionFactoryProvider hashFunctionFactoryProvider,
			IBinaryHashFunctionFamilyProvider hashFunctionFamilyProvider,
			IBinaryComparatorFactoryProvider comparatorFactoryProvider,
			ITypeTraitProvider typeTraitProvider,
			IBinaryBooleanInspectorFactory booleanInspectorFactory,
			IBinaryIntegerInspectorFactory integerInspectorFactory,
			IPrinterFactoryProvider printerFactoryProvider,
			INullWriterFactory nullWriterFactory,
			INormalizedKeyComputerFactoryProvider normalizedKeyComputerFactoryProvider,
			IExpressionRuntimeProvider expressionRuntimeProvider,
			IExpressionTypeComputer expressionTypeComputer,
			INullableTypeComputer nullableTypeComputer,
			ITypingContext typingContext,
			IExpressionEvalSizeComputer expressionEvalSizeComputer,
			IPartialAggregationTypeComputer partialAggregationTypeComputer,
			IPredicateEvaluatorFactoryProvider predEvaluatorFactoryProvider, int frameSize, AlgebricksPartitionConstraint clusterLocations) {
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
		this.nullWriterFactory = nullWriterFactory;
		this.expressionRuntimeProvider = expressionRuntimeProvider;
		this.expressionTypeComputer = expressionTypeComputer;
		this.typingContext = typingContext;
		this.expressionEvalSizeComputer = expressionEvalSizeComputer;
		this.partialAggregationTypeComputer = partialAggregationTypeComputer;
		this.predEvaluatorFactoryProvider = predEvaluatorFactoryProvider;
		this.frameSize = frameSize;
		this.varCounter = 0;
	}

	public IOperatorSchema getOuterFlowSchema() {
		return outerFlowSchema;
	}

	public AlgebricksPartitionConstraint getClusterLocations() {
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
	
	public IPredicateEvaluatorFactoryProvider getPredicateEvaluatorFactoryProvider(){
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

	public Object getType(ILogicalExpression expr, IVariableTypeEnvironment env)
			throws AlgebricksException {
		return expressionTypeComputer.getType(expr,
				typingContext.getMetadataProvider(), env);
	}

	public INullWriterFactory getNullWriterFactory() {
		return nullWriterFactory;
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

}
