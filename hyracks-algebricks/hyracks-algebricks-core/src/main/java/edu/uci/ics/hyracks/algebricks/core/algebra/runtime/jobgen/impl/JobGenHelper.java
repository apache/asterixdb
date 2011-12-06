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
package edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl;

import java.util.Collection;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IBinaryHashFunctionFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.INormalizedKeyComputerFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactoryProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.ITypeTraitProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;

public final class JobGenHelper {

    private static final Logger LOGGER = Logger.getLogger(JobGenHelper.class.getName());

    public static ITreeIndexFrameFactory createBTreeNSMInteriorFrameFactory(ITypeTrait[] typeTraits) {
        return new BTreeNSMInteriorFrameFactory(new TypeAwareTupleWriterFactory(typeTraits));
    }

    public static ITreeIndexFrameFactory createBTreeNSMLeafFrameFactory(ITypeTrait[] typeTraits) {
        return new BTreeNSMLeafFrameFactory(new TypeAwareTupleWriterFactory(typeTraits));
    }

    //    TODO: VRB: Commented out for now. Need to be uncommented once this 
    // is made compatible with the RTree interfaces.
    //    public static ITreeIndexFrameFactory createRTreeNSMInteriorFrameFactory(ITypeTrait[] typeTraits, int keyFields) {
    //        return new RTreeNSMInteriorFrameFactory(new RTreeTypeAwareTupleWriterFactory(typeTraits), keyFields);
    //    }
    //
    //    public static ITreeIndexFrameFactory createRTreeNSMLeafFrameFactory(ITypeTrait[] typeTraits, int keyFields) {
    //        return new RTreeNSMLeafFrameFactory(new RTreeTypeAwareTupleWriterFactory(typeTraits), keyFields);
    //    }

    @SuppressWarnings("unchecked")
    public static RecordDescriptor mkRecordDescriptor(ILogicalOperator op, IOperatorSchema opSchema,
            JobGenContext context) throws AlgebricksException {
        ISerializerDeserializer[] fields = new ISerializerDeserializer[opSchema.getSize()];
        ISerializerDeserializerProvider sdp = context.getSerializerDeserializerProvider();
        int i = 0;
        IVariableTypeEnvironment env = context.getTypeEnvironment(op);
        for (LogicalVariable var : opSchema) {
            Object t = env.getVarType(var);
            if (t == null) {
                LOGGER.warning("No type for variable " + var);
                // throw new AlgebricksException("No type for variable " + var);
            }
            fields[i] = sdp.getSerializerDeserializer(t);
            i++;
        }
        return new RecordDescriptor(fields);
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

    public static IBinaryComparatorFactory[] variablesToAscBinaryComparatorFactories(
            Collection<LogicalVariable> varLogical, IVariableTypeEnvironment env, JobGenContext context)
            throws AlgebricksException {
        IBinaryComparatorFactory[] compFactories = new IBinaryComparatorFactory[varLogical.size()];
        IBinaryComparatorFactoryProvider bcfProvider = context.getBinaryComparatorFactoryProvider();
        int i = 0;
        for (LogicalVariable v : varLogical) {
            Object type = env.getVarType(v);
            compFactories[i++] = bcfProvider.getBinaryComparatorFactory(type, OrderKind.ASC);
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
            return nkcfProvider.getNormalizedKeyComputerFactory(type, OrderKind.ASC);
        }
        return null;
    }

    public static ITypeTrait[] variablesToTypeTraits(Collection<LogicalVariable> varLogical,
            IVariableTypeEnvironment env, JobGenContext context) throws AlgebricksException {
        ITypeTrait[] typeTraits = new ITypeTrait[varLogical.size()];
        ITypeTraitProvider typeTraitProvider = context.getTypeTraitProvider();
        int i = 0;
        for (LogicalVariable v : varLogical) {
            Object type = env.getVarType(v);
            typeTraits[i++] = typeTraitProvider.getTypeTrait(type);
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
