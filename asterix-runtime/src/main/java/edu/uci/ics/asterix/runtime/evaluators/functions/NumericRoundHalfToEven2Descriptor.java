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
/*
 * Numeric function Round half to even
 * Author : Xiaoyu Ma@UC Irvine
 * 01/30/2012
 */
package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.math.BigDecimal;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.base.AMutableInt16;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.AMutableInt8;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class NumericRoundHalfToEven2Descriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new NumericRoundHalfToEven2Descriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.NUMERIC_ROUND_HALF_TO_EVEN2;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {

                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();
                    private ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval = args[0].createEvaluator(argOut);
                    private ICopyEvaluator precision = args[1].createEvaluator(argOut);

                    private byte serNullTypeTag = ATypeTag.NULL.serialize();
                    private byte serInt8TypeTag = ATypeTag.INT8.serialize();
                    private byte serInt16TypeTag = ATypeTag.INT16.serialize();
                    private byte serInt32TypeTag = ATypeTag.INT32.serialize();
                    private byte serInt64TypeTag = ATypeTag.INT64.serialize();
                    private byte serFloatTypeTag = ATypeTag.FLOAT.serialize();
                    private byte serDoubleTypeTag = ATypeTag.DOUBLE.serialize();

                    private AMutableDouble aDouble = new AMutableDouble(0);
                    private AMutableFloat aFloat = new AMutableFloat(0);
                    private AMutableInt64 aInt64 = new AMutableInt64(0);
                    private AMutableInt32 aInt32 = new AMutableInt32(0);
                    private AMutableInt16 aInt16 = new AMutableInt16((short) 0);
                    private AMutableInt8 aInt8 = new AMutableInt8((byte) 0);
                    @SuppressWarnings("rawtypes")
                    private ISerializerDeserializer serde;

                    private int getPrecision(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut.reset();
                        precision.evaluate(tuple);

                        if (argOut.getByteArray()[0] == serInt8TypeTag) {
                            return (int) AInt8SerializerDeserializer.getByte(argOut.getByteArray(), 1);
                        } else if (argOut.getByteArray()[0] == serInt16TypeTag) {
                            return (int) AInt16SerializerDeserializer.getShort(argOut.getByteArray(), 1);
                        } else if (argOut.getByteArray()[0] == serInt32TypeTag) {
                            return (int) AInt32SerializerDeserializer.getInt(argOut.getByteArray(), 1);
                        } else if (argOut.getByteArray()[0] == serInt64TypeTag) {
                            return (int) AInt64SerializerDeserializer.getLong(argOut.getByteArray(), 1);
                        } else {
                            throw new AlgebricksException(AsterixBuiltinFunctions.NUMERIC_ROUND_HALF_TO_EVEN2.getName()
                                    + ": the precision argument should be an INT8/INT16/INT32/INT64.");
                        }
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut.reset();
                        eval.evaluate(tuple);

                        try {
                            if (argOut.getByteArray()[0] == serNullTypeTag) {
                                serde = AqlSerializerDeserializerProvider.INSTANCE
                                        .getSerializerDeserializer(BuiltinType.ANULL);
                                serde.serialize(ANull.NULL, out);
                            } else if (argOut.getByteArray()[0] == serInt8TypeTag) {
                                serde = AqlSerializerDeserializerProvider.INSTANCE
                                        .getSerializerDeserializer(BuiltinType.AINT8);
                                byte val = (byte) AInt8SerializerDeserializer.getByte(argOut.getByteArray(), 1);
                                aInt8.setValue(val);
                                serde.serialize(aInt8, out);
                            } else if (argOut.getByteArray()[0] == serInt16TypeTag) {
                                serde = AqlSerializerDeserializerProvider.INSTANCE
                                        .getSerializerDeserializer(BuiltinType.AINT16);
                                short val = (short) AInt16SerializerDeserializer.getShort(argOut.getByteArray(), 1);
                                aInt16.setValue(val);
                                serde.serialize(aInt16, out);
                            } else if (argOut.getByteArray()[0] == serInt32TypeTag) {
                                serde = AqlSerializerDeserializerProvider.INSTANCE
                                        .getSerializerDeserializer(BuiltinType.AINT32);
                                int val = (int) AInt32SerializerDeserializer.getInt(argOut.getByteArray(), 1);
                                aInt32.setValue(val);
                                serde.serialize(aInt32, out);
                            } else if (argOut.getByteArray()[0] == serInt64TypeTag) {
                                serde = AqlSerializerDeserializerProvider.INSTANCE
                                        .getSerializerDeserializer(BuiltinType.AINT64);
                                long val = (long) AInt64SerializerDeserializer.getLong(argOut.getByteArray(), 1);
                                aInt64.setValue(val);
                                serde.serialize(aInt64, out);
                            } else if (argOut.getByteArray()[0] == serFloatTypeTag) {
                                serde = AqlSerializerDeserializerProvider.INSTANCE
                                        .getSerializerDeserializer(BuiltinType.AFLOAT);
                                float val = (float) AFloatSerializerDeserializer.getFloat(argOut.getByteArray(), 1);
                                if (Float.isNaN(val) || Float.isInfinite(val) || val == -0.0F || val == 0.0F) {
                                    aFloat.setValue(val);
                                    serde.serialize(aFloat, out);
                                } else {
                                    BigDecimal r = new BigDecimal(Float.toString(val));
                                    aFloat.setValue(r.setScale(getPrecision(tuple), BigDecimal.ROUND_HALF_EVEN)
                                            .floatValue());
                                    serde.serialize(aFloat, out);
                                }
                            } else if (argOut.getByteArray()[0] == serDoubleTypeTag) {
                                serde = AqlSerializerDeserializerProvider.INSTANCE
                                        .getSerializerDeserializer(BuiltinType.ADOUBLE);
                                double val = (double) ADoubleSerializerDeserializer.getDouble(argOut.getByteArray(), 1);
                                if (Double.isNaN(val) || Double.isInfinite(val) || val == -0.0D || val == 0.0D) {
                                    aDouble.setValue(val);
                                    serde.serialize(aDouble, out);
                                } else {
                                    BigDecimal r = new BigDecimal(Double.toString(val));
                                    aDouble.setValue(r.setScale(getPrecision(tuple), BigDecimal.ROUND_HALF_EVEN)
                                            .doubleValue());
                                    serde.serialize(aDouble, out);
                                }
                            } else {
                                throw new NotImplementedException(
                                        AsterixBuiltinFunctions.NUMERIC_ROUND_HALF_TO_EVEN2.getName()
                                                + ": not implemented for "
                                                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut
                                                        .getByteArray()[0]));
                            }
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                    }
                };
            }
        };
    }

}
