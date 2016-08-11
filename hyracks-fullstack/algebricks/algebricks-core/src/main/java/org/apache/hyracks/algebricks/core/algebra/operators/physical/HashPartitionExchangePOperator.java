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
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder.TargetConstraint;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.job.IConnectorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;

public class HashPartitionExchangePOperator extends AbstractExchangePOperator {

    private List<LogicalVariable> hashFields;
    private INodeDomain domain;

    public HashPartitionExchangePOperator(List<LogicalVariable> hashFields, INodeDomain domain) {
        this.hashFields = hashFields;
        this.domain = domain;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.HASH_PARTITION_EXCHANGE;
    }

    public List<LogicalVariable> getHashFields() {
        return hashFields;
    }

    public INodeDomain getDomain() {
        return domain;
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context) {
        IPartitioningProperty p = new UnorderedPartitionedProperty(new ListSet<LogicalVariable>(hashFields), domain);
        this.deliveredProperties = new StructuralPropertiesVector(p, null);
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) {
        return emptyUnaryRequirements();
    }

    @Override
    public String toString() {
        return getOperatorTag().toString() + " " + hashFields;
    }

    @Override
    public Pair<IConnectorDescriptor, TargetConstraint> createConnectorDescriptor(IConnectorDescriptorRegistry spec,
            ILogicalOperator op, IOperatorSchema opSchema, JobGenContext context) throws AlgebricksException {
        int[] keys = new int[hashFields.size()];
        IBinaryHashFunctionFactory[] hashFunctionFactories = new IBinaryHashFunctionFactory[hashFields.size()];
        int i = 0;
        IBinaryHashFunctionFactoryProvider hashFunProvider = context.getBinaryHashFunctionFactoryProvider();
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        for (LogicalVariable v : hashFields) {
            keys[i] = opSchema.findVariable(v);
            hashFunctionFactories[i] = hashFunProvider.getBinaryHashFunctionFactory(env.getVarType(v));
            ++i;
        }
        ITuplePartitionComputerFactory tpcf = new FieldHashPartitionComputerFactory(keys, hashFunctionFactories);
        IConnectorDescriptor conn = new MToNPartitioningConnectorDescriptor(spec, tpcf);
        return new Pair<>(conn, null);
    }

}
