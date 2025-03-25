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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.AMutableRectangle;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@MissingNullInOutFunction
public class CreateRectangleDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = CreateRectangleDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();

                    private IPointable inputArg0 = new VoidPointable();
                    private IPointable inputArg1 = new VoidPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    private AMutableRectangle aRectangle = new AMutableRectangle(null, null);
                    private AMutablePoint[] aPoint = { new AMutablePoint(0, 0), new AMutablePoint(0, 0) };
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ARectangle> rectangle2DSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ARECTANGLE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);

                        if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0, inputArg1)) {
                            return;
                        }

                        byte[] bytes0 = inputArg0.getByteArray();
                        int offset0 = inputArg0.getStartOffset();
                        byte[] bytes1 = inputArg1.getByteArray();
                        int offset1 = inputArg1.getStartOffset();

                        resultStorage.reset();
                        if (bytes0[offset0] != ATypeTag.SERIALIZED_POINT_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                                    ATypeTag.SERIALIZED_POINT_TYPE_TAG);
                        }
                        if (bytes1[offset1] != ATypeTag.SERIALIZED_POINT_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                                    ATypeTag.SERIALIZED_POINT_TYPE_TAG);
                        }

                        try {
                            aPoint[0]
                                    .setValue(
                                            ADoubleSerializerDeserializer.getDouble(bytes0,
                                                    offset0 + 1
                                                            + APointSerializerDeserializer
                                                                    .getCoordinateOffset(Coordinate.X)),
                                            ADoubleSerializerDeserializer.getDouble(bytes0, offset0 + 1
                                                    + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y)));
                            aPoint[1]
                                    .setValue(
                                            ADoubleSerializerDeserializer.getDouble(bytes1,
                                                    offset1 + 1
                                                            + APointSerializerDeserializer
                                                                    .getCoordinateOffset(Coordinate.X)),
                                            ADoubleSerializerDeserializer.getDouble(bytes1, offset1 + 1
                                                    + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y)));
                            if (aPoint[0].getX() > aPoint[1].getX() && aPoint[0].getY() > aPoint[1].getY()) {
                                aRectangle.setValue(aPoint[1], aPoint[0]);
                            } else if (aPoint[0].getX() < aPoint[1].getX() && aPoint[0].getY() < aPoint[1].getY()) {
                                aRectangle.setValue(aPoint[0], aPoint[1]);
                            } else {
                                throw new IllegalArgumentException(
                                        "Rectangle arugment must be either (bottom left point, top right point) or (top right point, bottom left point)");
                            }
                            rectangle2DSerde.serialize(aRectangle, out);
                            result.set(resultStorage);
                        } catch (IOException e1) {
                            throw HyracksDataException.create(e1);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.CREATE_RECTANGLE;
    }

}
