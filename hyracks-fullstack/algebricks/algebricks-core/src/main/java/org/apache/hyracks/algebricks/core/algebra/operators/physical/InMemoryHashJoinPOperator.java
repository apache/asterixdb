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

import java.util.LinkedList;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionRuntimeProvider;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator.JoinKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.TuplePairEvaluatorFactory;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactoryProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.join.InMemoryHashJoinOperatorDescriptor;

public class InMemoryHashJoinPOperator extends AbstractHashJoinPOperator {

    private final int tableSize;

    /**
     * builds on the first operator and probes on the second.
     */

    public InMemoryHashJoinPOperator(JoinKind kind, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeftOfEqualities, List<LogicalVariable> sideRightOfEqualities, int tableSize) {
        super(kind, partitioningType, sideLeftOfEqualities, sideRightOfEqualities);
        this.tableSize = tableSize;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.IN_MEMORY_HASH_JOIN;
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + " " + keysLeftBranch + keysRightBranch;
    }

    @Override
    public boolean isMicroOperator() {
        return false;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        validateNumKeys(keysLeftBranch, keysRightBranch);
        int[] keysLeft = JobGenHelper.variablesToFieldIndexes(keysLeftBranch, inputSchemas[0]);
        int[] keysRight = JobGenHelper.variablesToFieldIndexes(keysRightBranch, inputSchemas[1]);
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        IBinaryHashFunctionFactory[] leftHashFunFactories =
                JobGenHelper.variablesToBinaryHashFunctionFactories(keysLeftBranch, env, context);
        IBinaryHashFunctionFactory[] rightHashFunFactories =
                JobGenHelper.variablesToBinaryHashFunctionFactories(keysRightBranch, env, context);

        IPredicateEvaluatorFactoryProvider predEvaluatorFactoryProvider =
                context.getPredicateEvaluatorFactoryProvider();
        IPredicateEvaluatorFactory predEvaluatorFactory = predEvaluatorFactoryProvider == null ? null
                : predEvaluatorFactoryProvider.getPredicateEvaluatorFactory(keysLeft, keysRight);

        RecordDescriptor recDescriptor =
                JobGenHelper.mkRecordDescriptor(context.getTypeEnvironment(op), propagatedSchema, context);
        IOperatorSchema[] conditionInputSchemas = new IOperatorSchema[1];
        conditionInputSchemas[0] = propagatedSchema;
        IExpressionRuntimeProvider expressionRuntimeProvider = context.getExpressionRuntimeProvider();
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        IScalarEvaluatorFactory cond = expressionRuntimeProvider.createEvaluatorFactory(
                joinOp.getCondition().getValue(), context.getTypeEnvironment(op), conditionInputSchemas, context);
        ITuplePairComparatorFactory comparatorFactory =
                new TuplePairEvaluatorFactory(cond, false, context.getBinaryBooleanInspectorFactory());
        IOperatorDescriptorRegistry spec = builder.getJobSpec();
        IOperatorDescriptor opDesc;

        int memSizeInFrames = localMemoryRequirements.getMemoryBudgetInFrames();

        switch (kind) {
            case INNER:
                opDesc = new InMemoryHashJoinOperatorDescriptor(spec, keysLeft, keysRight, leftHashFunFactories,
                        rightHashFunFactories, comparatorFactory, recDescriptor, tableSize, predEvaluatorFactory,
                        memSizeInFrames);
                break;
            case LEFT_OUTER:
                IMissingWriterFactory[] nonMatchWriterFactories = new IMissingWriterFactory[inputSchemas[1].getSize()];
                for (int j = 0; j < nonMatchWriterFactories.length; j++) {
                    nonMatchWriterFactories[j] = context.getMissingWriterFactory();
                }
                opDesc = new InMemoryHashJoinOperatorDescriptor(spec, keysLeft, keysRight, leftHashFunFactories,
                        rightHashFunFactories, comparatorFactory, predEvaluatorFactory, recDescriptor, true,
                        nonMatchWriterFactories, tableSize, memSizeInFrames);
                break;
            default:
                throw new NotImplementedException();
        }

        opDesc.setSourceLocation(op.getSourceLocation());
        contributeOpDesc(builder, (AbstractLogicalOperator) op, opDesc);

        ILogicalOperator src1 = op.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src1, 0, op, 0);
        ILogicalOperator src2 = op.getInputs().get(1).getValue();
        builder.contributeGraphEdge(src2, 0, op, 1);
    }

    @Override
    protected List<ILocalStructuralProperty> deliveredLocalProperties(ILogicalOperator op,
            IOptimizationContext context) {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) op.getInputs().get(0).getValue();
        IPhysicalPropertiesVector pv0 = op0.getPhysicalOperator().getDeliveredProperties();
        List<ILocalStructuralProperty> lp0 = pv0.getLocalProperties();
        if (lp0 != null) {
            // maintains the local properties on the probe side
            return new LinkedList<>(lp0);
        }
        return new LinkedList<>();
    }

}
