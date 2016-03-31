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

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.AMutableRectangle;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SpatialCellDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SpatialCellDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws AlgebricksException {
                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IPointable inputArg0 = new VoidPointable();
                    private final IPointable inputArg1 = new VoidPointable();
                    private final IPointable inputArg2 = new VoidPointable();
                    private final IPointable inputArg3 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval2 = args[2].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval3 = args[3].createScalarEvaluator(ctx);
                    private final AMutableRectangle aRectangle = new AMutableRectangle(null, null);
                    private final AMutablePoint[] aPoint = { new AMutablePoint(0, 0), new AMutablePoint(0, 0) };

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ARectangle> rectangleSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ARECTANGLE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);
                        eval2.evaluate(tuple, inputArg2);
                        eval3.evaluate(tuple, inputArg3);

                        byte[] bytes0 = inputArg0.getByteArray();
                        byte[] bytes1 = inputArg1.getByteArray();
                        byte[] bytes2 = inputArg2.getByteArray();
                        byte[] bytes3 = inputArg3.getByteArray();
                        int offset0 = inputArg0.getStartOffset();
                        int offset1 = inputArg1.getStartOffset();
                        int offset2 = inputArg2.getStartOffset();
                        int offset3 = inputArg3.getStartOffset();

                        try {
                            ATypeTag tag0 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]);
                            ATypeTag tag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]);
                            ATypeTag tag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes2[offset2]);
                            ATypeTag tag3 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes3[offset3]);
                            if (tag0 == ATypeTag.POINT && tag1 == ATypeTag.POINT && tag2 == ATypeTag.DOUBLE
                                    && tag3 == ATypeTag.DOUBLE) {
                                double xLoc = ADoubleSerializerDeserializer.getDouble(bytes0,
                                        offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                double yLoc = ADoubleSerializerDeserializer.getDouble(bytes0,
                                        offset0 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                                double xOrigin = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        offset1 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                double yOrigin = ADoubleSerializerDeserializer.getDouble(bytes1,
                                        offset1 + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                                double xInc = ADoubleSerializerDeserializer.getDouble(bytes2, offset2 + 1);
                                double yInc = ADoubleSerializerDeserializer.getDouble(bytes3, offset3 + 1);

                                double x = xOrigin + (Math.floor((xLoc - xOrigin) / xInc)) * xInc;
                                double y = yOrigin + (Math.floor((yLoc - yOrigin) / yInc)) * yInc;
                                aPoint[0].setValue(x, y);
                                aPoint[1].setValue(x + xInc, y + yInc);
                                aRectangle.setValue(aPoint[0], aPoint[1]);
                                rectangleSerde.serialize(aRectangle, out);
                            } else if (tag0 == ATypeTag.NULL || tag1 == ATypeTag.NULL || tag2 == ATypeTag.NULL
                                    || tag3 == ATypeTag.NULL) {
                                nullSerde.serialize(ANull.NULL, out);
                            } else {
                                throw new AlgebricksException(AsterixBuiltinFunctions.SPATIAL_CELL.getName()
                                        + ": expects input type: (POINT, POINT, DOUBLE, DOUBLE) but got ("
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]) + ", "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]) + ", "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes2[offset2]) + ", "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes3[offset3]) + ").");
                            }
                            result.set(resultStorage);
                        } catch (IOException e1) {
                            throw new AlgebricksException(e1);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SPATIAL_CELL;
    }

}
