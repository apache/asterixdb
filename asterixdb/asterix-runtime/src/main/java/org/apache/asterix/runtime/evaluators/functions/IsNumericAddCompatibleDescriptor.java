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

package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.AbstractMultiTypeCheckEvaluator;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Returns {@code TRUE} if the argument type is one of the types that are allowed on the left side of
 * {@link BuiltinFunctions#NUMERIC_ADD numeric-add()}:
 * <ul>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#AMISSING MISSING}</li>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#ANULL NULL}</li>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#AINT8 TINYINT}</li>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#AINT16 SMALLINT}</li>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#AINT32 INTEGER}</li>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#AINT64 BIGINT}</li>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#AFLOAT FLOAT}</li>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#ADOUBLE DOUBLE}</li>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#ADATE DATE}</li>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#ADATETIME DATETIME}</li>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#ATIME TIME}</li>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#ADURATION DURATION}</li>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#AYEARMONTHDURATION YEARMONTHDURATION}</li>
 *     <li>{@link org.apache.asterix.om.types.BuiltinType#ADAYTIMEDURATION DAYTIMEDURATION}</li>
 * </ul>
 *
 * Returns {@code FALSE} for all other types
 *
 * @see NumericAddDescriptor
 * @see AbstractNumericArithmeticEval
 */
public class IsNumericAddCompatibleDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    public static final IFunctionDescriptorFactory FACTORY = IsNumericAddCompatibleDescriptor::new;
    private static final long serialVersionUID = 1L;
    private static final byte[] checkedTypes = new byte[] { ATypeTag.SERIALIZED_MISSING_TYPE_TAG,
            ATypeTag.SERIALIZED_NULL_TYPE_TAG, ATypeTag.SERIALIZED_INT8_TYPE_TAG, ATypeTag.SERIALIZED_INT16_TYPE_TAG,
            ATypeTag.SERIALIZED_INT32_TYPE_TAG, ATypeTag.SERIALIZED_INT64_TYPE_TAG, ATypeTag.SERIALIZED_FLOAT_TYPE_TAG,
            ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG,
            ATypeTag.SERIALIZED_TIME_TYPE_TAG, ATypeTag.SERIALIZED_DATE_TYPE_TAG,
            ATypeTag.SERIALIZED_YEAR_MONTH_DURATION_TYPE_TAG, ATypeTag.SERIALIZED_DAY_TIME_DURATION_TYPE_TAG,
            ATypeTag.SERIALIZED_DURATION_TYPE_TAG };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractMultiTypeCheckEvaluator(args[0].createScalarEvaluator(ctx), checkedTypes) {
                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        eval.evaluate(tuple, argPtr);
                        res = isMatch(argPtr.getByteArray()[argPtr.getStartOffset()]) ? ABoolean.TRUE : ABoolean.FALSE;
                        resultStorage.reset();
                        aObjectSerializerDeserializer.serialize(res, out);
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.IS_NUMERIC_ADD_COMPATIBLE;
    }
}
