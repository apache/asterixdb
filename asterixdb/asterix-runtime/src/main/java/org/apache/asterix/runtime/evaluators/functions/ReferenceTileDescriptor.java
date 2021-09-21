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

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ReferenceTileDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ReferenceTileDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.REFERENCE_TILE;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {
                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final IPointable inputArg0 = new VoidPointable();
                    private final IPointable inputArg1 = new VoidPointable();
                    private final IPointable inputArg2 = new VoidPointable();
                    private final IPointable inputArg3 = new VoidPointable();
                    private final IPointable inputArg4 = new VoidPointable();
                    private final IPointable inputArg5 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval2 = args[2].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval3 = args[3].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval4 = args[4].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval5 = args[5].createScalarEvaluator(ctx);

                    private final AMutableInt32 aInt32 = new AMutableInt32(0);

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer intSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        eval0.evaluate(tuple, inputArg0);
                        eval1.evaluate(tuple, inputArg1);
                        eval2.evaluate(tuple, inputArg2);
                        eval3.evaluate(tuple, inputArg3);
                        eval4.evaluate(tuple, inputArg4);
                        eval5.evaluate(tuple, inputArg5);

                        if (PointableHelper.checkAndSetMissingOrNull(result, inputArg0, inputArg1, inputArg2, inputArg3,
                                inputArg4)) {
                            return;
                        }

                        byte[] bytes0 = inputArg0.getByteArray();
                        byte[] bytes1 = inputArg1.getByteArray();
                        byte[] bytes2 = inputArg2.getByteArray();
                        byte[] bytes3 = inputArg3.getByteArray();
                        byte[] bytes4 = inputArg4.getByteArray();

                        int offset0 = inputArg0.getStartOffset();
                        int offset1 = inputArg1.getStartOffset();
                        int offset2 = inputArg2.getStartOffset();
                        int offset3 = inputArg3.getStartOffset();
                        int offset4 = inputArg4.getStartOffset();

                        ATypeTag tag0 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]);
                        ATypeTag tag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]);
                        ATypeTag tag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes2[offset2]);

                        if (tag0 != ATypeTag.RECTANGLE) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                                    ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG);
                        }
                        if (tag1 != ATypeTag.RECTANGLE) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes1[offset1],
                                    ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG);
                        }
                        if (tag2 != ATypeTag.RECTANGLE) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes2[offset2],
                                    ATypeTag.SERIALIZED_RECTANGLE_TYPE_TAG);
                        }

                        double ax1 = ADoubleSerializerDeserializer.getDouble(bytes0, offset0 + 1
                                + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                        double ay1 = ADoubleSerializerDeserializer.getDouble(bytes0, offset0 + 1
                                + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));

                        double bx1 = ADoubleSerializerDeserializer.getDouble(bytes1, offset1 + 1
                                + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                        double by1 = ADoubleSerializerDeserializer.getDouble(bytes1, offset1 + 1
                                + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));

                        double minX = ADoubleSerializerDeserializer.getDouble(bytes2, offset2 + 1
                                + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
                        double minY = ADoubleSerializerDeserializer.getDouble(bytes2, offset2 + 1
                                + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));

                        double maxX = ADoubleSerializerDeserializer.getDouble(bytes2, offset2 + 1
                                + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
                        double maxY = ADoubleSerializerDeserializer.getDouble(bytes2, offset2 + 1
                                + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));

                        int rows = ATypeHierarchy.getIntegerValue(getIdentifier().getName(), 0, bytes3, offset3);
                        int columns = ATypeHierarchy.getIntegerValue(getIdentifier().getName(), 0, bytes4, offset4);

                        // Compute the reference point
                        double x = Math.max(ax1, bx1);
                        double y = Math.max(ay1, by1);

                        // Compute the tile ID of the reference point
                        int row = (int) Math.ceil((y - minY) * rows / (maxY - minY));
                        int col = (int) Math.ceil((x - minX) * columns / (maxX - minX));

                        row = Math.min(Math.max(1, row), rows * columns);
                        col = Math.min(Math.max(1, col), rows * columns);

                        int tileId = (row - 1) * columns + col;
                        resultStorage.reset();
                        aInt32.setValue(tileId);
                        intSerde.serialize(aInt32, resultStorage.getDataOutput());
                        result.set(resultStorage);
                    }
                };
            }
        };
    }
}
