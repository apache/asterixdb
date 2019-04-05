
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

package org.apache.asterix.runtime;

import static org.mockito.Mockito.mock;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionCollection;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.junit.Assert;
import org.junit.Test;

public class NullMissingTest {

    @Test
    public void test() throws Exception {
        List<IFunctionDescriptorFactory> functions =
                FunctionCollection.createDefaultFunctionCollection().getFunctionDescriptorFactories();
        int testedFunctions = 0;
        Set<FunctionIdentifier> functionsRequiringTypes = new HashSet<>();
        buildFunctionsRequiringTypes(functionsRequiringTypes);
        for (IFunctionDescriptorFactory func : functions) {
            String className = func.getClass().getName();
            // We test all generated functions except
            // record and cast functions, which requires type settings (we test them in runtime tests).
            if (className.contains("Gen") && !className.contains("record") && !className.contains("Cast")) {
                testFunction(func, className, functionsRequiringTypes);
                ++testedFunctions;
            }
        }
        // 217 is the current number of functions with generated code.
        Assert.assertTrue("expected >= 217 generated functions to be tested, but was " + testedFunctions,
                testedFunctions >= 217);
    }

    private void testFunction(IFunctionDescriptorFactory funcFactory, String className,
            Set<FunctionIdentifier> functionsRequiringTypes) throws Exception {
        IFunctionDescriptor functionDescriptor = funcFactory.createFunctionDescriptor();
        if (!(functionDescriptor instanceof AbstractScalarFunctionDynamicDescriptor)) {
            System.out.println("Excluding " + className);
            return;
        }
        System.out.println("Testing " + className);
        AbstractScalarFunctionDynamicDescriptor funcDesc = (AbstractScalarFunctionDynamicDescriptor) functionDescriptor;
        int inputArity = funcDesc.getIdentifier().getArity();
        boolean t = functionsRequiringTypes.contains(funcDesc.getIdentifier()); // whether to build args types or not
        Iterator<Pair<IScalarEvaluatorFactory[], IAType[]>> argEvalFactoryIterator = getArgCombinations(inputArity, t);
        int index = 0;
        while (argEvalFactoryIterator.hasNext()) {
            Pair<IScalarEvaluatorFactory[], IAType[]> next = argEvalFactoryIterator.next();
            if (next.second != null) {
                funcDesc.setImmutableStates((Object[]) next.second);
            }
            IScalarEvaluatorFactory evalFactory = funcDesc.createEvaluatorFactory(next.first);
            IHyracksTaskContext ctx = mock(IHyracksTaskContext.class);
            IScalarEvaluator evaluator = evalFactory.createScalarEvaluator(ctx);
            IPointable resultPointable = new VoidPointable();
            evaluator.evaluate(null, resultPointable);
            if (index != 0) {
                Assert.assertTrue(resultPointable.getByteArray()[resultPointable
                        .getStartOffset()] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
            } else {
                Assert.assertTrue(resultPointable.getByteArray()[resultPointable
                        .getStartOffset()] == ATypeTag.SERIALIZED_NULL_TYPE_TAG);
            }
            ++index;
        }
    }

    private Iterator<Pair<IScalarEvaluatorFactory[], IAType[]>> getArgCombinations(int inputArity, boolean buildTypes) {
        int argSize = inputArity >= 0 ? inputArity : 3;
        final int numCombinations = 1 << argSize;
        return new Iterator<Pair<IScalarEvaluatorFactory[], IAType[]>>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < numCombinations;
            }

            @Override
            public Pair<IScalarEvaluatorFactory[], IAType[]> next() {
                IScalarEvaluatorFactory[] scalarEvaluatorFactories = new IScalarEvaluatorFactory[argSize];
                IAType[] argsTypes = buildTypes ? new IAType[argSize] : null;
                for (int j = 0; j < argSize; ++j) {
                    IAType type = (index & (1 << j)) != 0 ? BuiltinType.AMISSING : BuiltinType.ANULL;
                    scalarEvaluatorFactories[j] = new ConstantEvalFactory(new byte[] { type.getTypeTag().serialize() });
                    if (buildTypes) {
                        argsTypes[j] = type;
                    }
                }
                ++index;
                return new Pair<>(scalarEvaluatorFactories, argsTypes);
            }

        };

    }

    // those are the functions that need IATypes of their args and use them in the function constructor
    private void buildFunctionsRequiringTypes(Set<FunctionIdentifier> functionsRequiringTypes) {
        functionsRequiringTypes.add(BuiltinFunctions.ARRAY_POSITION);
        functionsRequiringTypes.add(BuiltinFunctions.ARRAY_CONTAINS);
        functionsRequiringTypes.add(BuiltinFunctions.ARRAY_SORT);
        functionsRequiringTypes.add(BuiltinFunctions.ARRAY_DISTINCT);
        functionsRequiringTypes.add(BuiltinFunctions.EQ);
        functionsRequiringTypes.add(BuiltinFunctions.LT);
        functionsRequiringTypes.add(BuiltinFunctions.GT);
        functionsRequiringTypes.add(BuiltinFunctions.GE);
        functionsRequiringTypes.add(BuiltinFunctions.LE);
        functionsRequiringTypes.add(BuiltinFunctions.NEQ);
        functionsRequiringTypes.add(BuiltinFunctions.MISSING_IF);
        functionsRequiringTypes.add(BuiltinFunctions.NAN_IF);
        functionsRequiringTypes.add(BuiltinFunctions.NEGINF_IF);
        functionsRequiringTypes.add(BuiltinFunctions.NULL_IF);
        functionsRequiringTypes.add(BuiltinFunctions.POSINF_IF);
    }
}
