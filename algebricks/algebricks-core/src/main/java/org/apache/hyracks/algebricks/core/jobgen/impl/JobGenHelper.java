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

import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IBinaryHashFunctionFamilyProvider;
import edu.uci.ics.hyracks.algebricks.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactoryProvider;
import edu.uci.ics.hyracks.algebricks.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.algebricks.data.ITypeTraitProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public final class JobGenHelper {

    private static final Logger LOGGER = Logger.getLogger(JobGenHelper.class.getName());

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
                LOGGER.warning("No type for variable " + var);
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
        for (int i = 0; i < pf.length; i++) {
            LogicalVariable v = opSchema.getVariable(printColumns[i]);
            Object t = env.getVarType(v);
            pf[i] = pff.getPrinterFactory(t);
        }
        return pf;
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
        int[] projectionList = new int[opSchema.getSize()];
        int k = 0;
        for (LogicalVariable v : opSchema) {
            projectionList[k++] = opSchema.findVariable(v);
        }
        return projectionList;
    }

}
