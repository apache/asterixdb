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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.functions.CastTypeLaxDescriptor;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ConstantEvalFactory;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class CastTypeLaxTest {

    private IAType inType;

    private IAObject inValue;

    private IAObject targetType;

    private IAObject targetValue;

    public CastTypeLaxTest(IAType inType, IAObject inValue, IAObject targetType, IAObject targetValue) {
        this.inType = inType;
        this.inValue = inValue;
        this.targetType = targetType;
        this.targetValue = targetValue;
    }

    @Test
    public void testCastLax() throws Exception {
        IFunctionDescriptor funcDesc = CastTypeLaxDescriptor.FACTORY.createFunctionDescriptor();

        funcDesc.setImmutableStates(targetType, inType);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        AObjectSerializerDeserializer serDe = AObjectSerializerDeserializer.INSTANCE;
        serDe.serialize(inValue, new DataOutputStream(baos));

        ConstantEvalFactory argEvalFactory = new ConstantEvalFactory(baos.toByteArray());
        IScalarEvaluatorFactory evalFactory =
                funcDesc.createEvaluatorFactory(new IScalarEvaluatorFactory[] { argEvalFactory });
        IEvaluatorContext ctx = mock(IEvaluatorContext.class);
        IScalarEvaluator evaluator = evalFactory.createScalarEvaluator(ctx);
        VoidPointable resultPointable = new VoidPointable();
        evaluator.evaluate(null, resultPointable);

        ByteArrayInputStream bais = new ByteArrayInputStream(resultPointable.getByteArray(),
                resultPointable.getStartOffset(), resultPointable.getLength());
        IAObject resultValue = serDe.deserialize(new DataInputStream(bais));

        Assert.assertTrue(String.format("Expected: %s, actual: %s", targetValue, resultValue),
                targetValue.deepEqual(resultValue));
    }

    @Parameterized.Parameters(name = "CastTypeLaxTest {index}: {0}({1})->{2}({3})")
    public static Collection<Object[]> tests() throws Exception {
        List<Object[]> tests = new ArrayList<>();

        IAType[] numericTypes = new IAType[] { BuiltinType.AINT8, BuiltinType.AINT16, BuiltinType.AINT32,
                BuiltinType.AINT64, BuiltinType.AFLOAT, BuiltinType.ADOUBLE };

        // numeric -> numeric
        for (IAType inType : numericTypes) {
            for (IAType targetType : numericTypes) {
                if (!inType.equals(targetType)) {
                    generateTests(tests, inType, targetType);
                }
            }
        }

        // type mismatch -> missing
        addTest(tests, BuiltinType.ABOOLEAN, ABoolean.TRUE, BuiltinType.ADATE, AMissing.MISSING);

        return tests;
    }

    private static void generateTests(List<Object[]> outTests, IAType inType, IAType targetType) {
        int value = inType.getTypeTag().ordinal();
        addTest(outTests, inType, createValue(inType, value), targetType, createValue(targetType, value));

        if (ATypeHierarchy.canDemote(inType.getTypeTag(), targetType.getTypeTag())) {
            IAObject inMax = createValue(inType, getMax(inType));
            IAObject inMin = createValue(inType, getMin(inType));
            IAObject targetMax = createValue(targetType, getMax(targetType));
            IAObject targetMin = createValue(targetType, getMin(targetType));

            addTest(outTests, inType, inMax, targetType, targetMax);
            addTest(outTests, inType, inMin, targetType, targetMin);

            if (!isInteger(inType) && isInteger(targetType)) {
                addTest(outTests, inType, createValue(inType, getPositiveInfinity(inType)), targetType, targetMax);
                addTest(outTests, inType, createValue(inType, getNegativeInfinity(inType)), targetType, targetMin);
                addTest(outTests, inType, createValue(inType, getNaN(inType)), targetType, createValue(targetType, 0));
            }
        }
    }

    private static void addTest(List<Object[]> outTests, IAType inType, IAObject inValue, IAObject targetType,
            IAObject targetValue) {
        outTests.add(new Object[] { inType, inValue, targetType, targetValue });
    }

    private static IAObject createValue(IAType type, Number value) {
        switch (type.getTypeTag()) {
            case TINYINT:
                return new AInt8(value.byteValue());
            case SMALLINT:
                return new AInt16(value.shortValue());
            case INTEGER:
                return new AInt32(value.intValue());
            case BIGINT:
                return new AInt64(value.longValue());
            case FLOAT:
                return new AFloat(value.floatValue());
            case DOUBLE:
                return new ADouble(value.doubleValue());
            default:
                throw new IllegalStateException(type.toString());
        }
    }

    private static Number getMax(IAType type) {
        switch (type.getTypeTag()) {
            case TINYINT:
                return Byte.MAX_VALUE;
            case SMALLINT:
                return Short.MAX_VALUE;
            case INTEGER:
                return Integer.MAX_VALUE;
            case BIGINT:
                return Long.MAX_VALUE;
            case FLOAT:
                return Float.MAX_VALUE;
            case DOUBLE:
                return Double.MAX_VALUE;
            default:
                throw new IllegalStateException(type.toString());
        }
    }

    private static Number getMin(IAType type) {
        switch (type.getTypeTag()) {
            case TINYINT:
                return Byte.MIN_VALUE;
            case SMALLINT:
                return Short.MIN_VALUE;
            case INTEGER:
                return Integer.MIN_VALUE;
            case BIGINT:
                return Long.MIN_VALUE;
            case FLOAT:
                return -Float.MAX_VALUE;
            case DOUBLE:
                return -Double.MAX_VALUE;
            default:
                throw new IllegalStateException(type.toString());
        }
    }

    private static Number getPositiveInfinity(IAType type) {
        switch (type.getTypeTag()) {
            case FLOAT:
                return Float.POSITIVE_INFINITY;
            case DOUBLE:
                return Double.POSITIVE_INFINITY;
            default:
                throw new IllegalStateException(type.toString());
        }
    }

    private static Number getNegativeInfinity(IAType type) {
        switch (type.getTypeTag()) {
            case FLOAT:
                return Float.NEGATIVE_INFINITY;
            case DOUBLE:
                return Double.NEGATIVE_INFINITY;
            default:
                throw new IllegalStateException(type.toString());
        }
    }

    private static Number getNaN(IAType type) {
        switch (type.getTypeTag()) {
            case FLOAT:
                return Float.NaN;
            case DOUBLE:
                return Double.NaN;
            default:
                throw new IllegalStateException(type.toString());
        }
    }

    private static boolean isInteger(IAType type) {
        switch (type.getTypeTag()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return true;
            default:
                return false;
        }
    }
}
