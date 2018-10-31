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

import java.util.Collection;
import java.util.List;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import org.apache.hyracks.algebricks.data.IBinaryHashFunctionFamilyProvider;
import org.apache.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class JobGenHelper {

    private static final Logger LOGGER = LogManager.getLogger();

    @SuppressWarnings("rawtypes")
    public static RecordDescriptor mkRecordDescriptor(IVariableTypeEnvironment env, IOperatorSchema opSchema,
            JobGenContext context) throws AlgebricksException {
        ISerializerDeserializer[] fields = new ISerializerDeserializer[opSchema.getSize()];
        ITypeTraits[] typeTraits = new ITypeTraits[opSchema.getSize()];
        ISerializerDeserializerProvider sdp = context.getSerializerDeserializerProvider();
        ITypeTraitProvider ttp = context.getTypeTraitProvider();
        int i = 0;
        for (LogicalVariable var : opSchema) {
            Object t = env.getVarType(var);
            if (t == null) {
                LOGGER.warn("No type for variable " + var);
            }
            fields[i] = sdp.getSerializerDeserializer(t);
            typeTraits[i] = ttp.getTypeTrait(t);
            i++;
        }
        return new RecordDescriptor(fields, typeTraits);
    }

    public static IPrinterFactory[] mkPrinterFactories(IOperatorSchema opSchema, IVariableTypeEnvironment env,
            JobGenContext context, int[] printColumns) throws AlgebricksException {
        IPrinterFactory[] pf = new IPrinterFactory[printColumns.length];
        IPrinterFactoryProvider pff = context.getPrinterFactoryProvider();
        try {
            for (int i = 0; i < pf.length; i++) {
                LogicalVariable v = opSchema.getVariable(printColumns[i]);
                Object t = env.getVarType(v);
                pf[i] = pff.getPrinterFactory(t);
            }
            return pf;
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    public static int[] variablesToFieldIndexes(Collection<LogicalVariable> varLogical, IOperatorSchema opSchema) {
        int[] tuplePos = new int[varLogical.size()];
        int i = 0;
        for (LogicalVariable var : varLogical) {
            tuplePos[i] = opSchema.findVariable(var);
            i++;
        }
        return tuplePos;
    }

    public static IBinaryHashFunctionFactory[] variablesToBinaryHashFunctionFactories(
            Collection<LogicalVariable> varLogical, IVariableTypeEnvironment env, JobGenContext context)
            throws AlgebricksException {
        IBinaryHashFunctionFactory[] funFactories = new IBinaryHashFunctionFactory[varLogical.size()];
        int i = 0;
        IBinaryHashFunctionFactoryProvider bhffProvider = context.getBinaryHashFunctionFactoryProvider();
        for (LogicalVariable var : varLogical) {
            Object type = env.getVarType(var);
            funFactories[i++] = bhffProvider.getBinaryHashFunctionFactory(type);
        }
        return funFactories;
    }

    public static IBinaryHashFunctionFamily[] variablesToBinaryHashFunctionFamilies(
            Collection<LogicalVariable> varLogical, IVariableTypeEnvironment env, JobGenContext context)
            throws AlgebricksException {
        IBinaryHashFunctionFamily[] funFamilies = new IBinaryHashFunctionFamily[varLogical.size()];
        int i = 0;
        IBinaryHashFunctionFamilyProvider bhffProvider = context.getBinaryHashFunctionFamilyProvider();
        for (LogicalVariable var : varLogical) {
            Object type = env.getVarType(var);
            funFamilies[i++] = bhffProvider.getBinaryHashFunctionFamily(type);
        }
        return funFamilies;
    }

    public static IBinaryComparatorFactory[] variablesToAscBinaryComparatorFactories(
            Collection<LogicalVariable> varLogical, IVariableTypeEnvironment env, JobGenContext context)
            throws AlgebricksException {
        IBinaryComparatorFactory[] compFactories = new IBinaryComparatorFactory[varLogical.size()];
        IBinaryComparatorFactoryProvider bcfProvider = context.getBinaryComparatorFactoryProvider();
        int i = 0;
        for (LogicalVariable v : varLogical) {
            Object type = env.getVarType(v);
            compFactories[i++] = bcfProvider.getBinaryComparatorFactory(type, true);
        }
        return compFactories;
    }

    public static IBinaryComparatorFactory[] variablesToAscBinaryComparatorFactories(List<LogicalVariable> varLogical,
            int start, int size, IVariableTypeEnvironment env, JobGenContext context) throws AlgebricksException {
        IBinaryComparatorFactory[] compFactories = new IBinaryComparatorFactory[size];
        IBinaryComparatorFactoryProvider bcfProvider = context.getBinaryComparatorFactoryProvider();
        for (int i = 0; i < size; i++) {
            Object type = env.getVarType(varLogical.get(start + i));
            compFactories[i] = bcfProvider.getBinaryComparatorFactory(type, true);
        }
        return compFactories;
    }

    public static IBinaryComparatorFactory[] variablesToBinaryComparatorFactories(Collection<OrderColumn> orderColumns,
            IVariableTypeEnvironment env, JobGenContext context) throws AlgebricksException {
        IBinaryComparatorFactory[] compFactories = new IBinaryComparatorFactory[orderColumns.size()];
        IBinaryComparatorFactoryProvider bcfProvider = context.getBinaryComparatorFactoryProvider();
        int i = 0;
        for (OrderColumn oc : orderColumns) {
            LogicalVariable v = oc.getColumn();
            boolean ascending = oc.getOrder() == OrderOperator.IOrder.OrderKind.ASC;
            Object type = env.getVarType(v);
            compFactories[i++] = bcfProvider.getBinaryComparatorFactory(type, ascending);
        }
        return compFactories;
    }

    public static INormalizedKeyComputerFactory variablesToAscNormalizedKeyComputerFactory(
            Collection<LogicalVariable> varLogical, IVariableTypeEnvironment env, JobGenContext context)
            throws AlgebricksException {
        INormalizedKeyComputerFactoryProvider nkcfProvider = context.getNormalizedKeyComputerFactoryProvider();
        if (nkcfProvider == null)
            return null;
        for (LogicalVariable v : varLogical) {
            Object type = env.getVarType(v);
            return nkcfProvider.getNormalizedKeyComputerFactory(type, true);
        }
        return null;
    }

    public static ITypeTraits[] variablesToTypeTraits(Collection<LogicalVariable> varLogical,
            IVariableTypeEnvironment env, JobGenContext context) throws AlgebricksException {
        ITypeTraits[] typeTraits = new ITypeTraits[varLogical.size()];
        ITypeTraitProvider typeTraitProvider = context.getTypeTraitProvider();
        int i = 0;
        for (LogicalVariable v : varLogical) {
            Object type = env.getVarType(v);
            typeTraits[i++] = typeTraitProvider.getTypeTrait(type);
        }
        return typeTraits;
    }

    public static ITypeTraits[] variablesToTypeTraits(List<LogicalVariable> varLogical, int start, int size,
            IVariableTypeEnvironment env, JobGenContext context) throws AlgebricksException {
        ITypeTraits[] typeTraits = new ITypeTraits[size];
        ITypeTraitProvider typeTraitProvider = context.getTypeTraitProvider();
        for (int i = 0; i < size; i++) {
            Object type = env.getVarType(varLogical.get(start + i));
            typeTraits[i] = typeTraitProvider.getTypeTrait(type);
        }
        return typeTraits;
    }

    public static int[] projectAllVariables(IOperatorSchema opSchema) {
        return projectVariablesImpl(opSchema, opSchema, opSchema.getSize());
    }

    public static int[] projectVariables(IOperatorSchema opSchema, List<LogicalVariable> variables) {
        return projectVariablesImpl(opSchema, variables, variables.size());
    }

    private static int[] projectVariablesImpl(IOperatorSchema opSchema, Iterable<LogicalVariable> variables,
            int variableCount) {
        int[] projectionList = new int[variableCount];
        int k = 0;
        for (LogicalVariable v : variables) {
            projectionList[k++] = opSchema.findVariable(v);
        }
        return projectionList;
    }
}
