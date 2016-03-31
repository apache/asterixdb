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
package org.apache.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ALineSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APolygonSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class CreateMBREvalFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private IScalarEvaluatorFactory recordEvalFactory;
    private IScalarEvaluatorFactory dimensionEvalFactory;
    private IScalarEvaluatorFactory coordinateEvalFactory;

    public CreateMBREvalFactory(IScalarEvaluatorFactory recordEvalFactory, IScalarEvaluatorFactory dimensionEvalFactory,
            IScalarEvaluatorFactory coordinateEvalFactory) {
        this.recordEvalFactory = recordEvalFactory;
        this.dimensionEvalFactory = dimensionEvalFactory;
        this.coordinateEvalFactory = coordinateEvalFactory;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
        return new IScalarEvaluator() {
            private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
            private final DataOutput out = resultStorage.getDataOutput();
            private final IPointable inputArg0 = new VoidPointable();
            private final IPointable inputArg1 = new VoidPointable();
            private final IPointable inputArg2 = new VoidPointable();

            private IScalarEvaluator eval0 = recordEvalFactory.createScalarEvaluator(ctx);
            private IScalarEvaluator eval1 = dimensionEvalFactory.createScalarEvaluator(ctx);
            private IScalarEvaluator eval2 = coordinateEvalFactory.createScalarEvaluator(ctx);

            @Override
            public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                eval0.evaluate(tuple, inputArg0);
                eval1.evaluate(tuple, inputArg1);
                eval2.evaluate(tuple, inputArg2);
                int startOffset0 = inputArg0.getStartOffset();
                int startOffset1 = inputArg1.getStartOffset();
                int startOffset2 = inputArg2.getStartOffset();

                resultStorage.reset();
                // type-check: (Point/Line/Polygon/Circle/Rectangle/Null, Int32, Int32)
                if (inputArg1.getByteArray()[startOffset1] != ATypeTag.SERIALIZED_INT32_TYPE_TAG
                        || inputArg2.getByteArray()[startOffset2] != ATypeTag.SERIALIZED_INT32_TYPE_TAG) {
                    throw new AlgebricksException(
                            "Expects Types: (Point/Line/Polygon/Circle/Rectangle/Null, Int32, Int32).");
                }

                try {
                    int dimension = AInt32SerializerDeserializer.getInt(inputArg1.getByteArray(), startOffset1 + 1);
                    int coordinate = AInt32SerializerDeserializer.getInt(inputArg2.getByteArray(), startOffset2 + 1);
                    double value;
                    if (dimension == 2) {
                        ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER
                                .deserialize(inputArg0.getByteArray()[startOffset0]);
                        switch (tag) {
                            case POINT:
                                switch (coordinate) {
                                    case 0: // 0 is for min x, 1 is for min y, 2
                                            // for
                                            // max x, and 3 for max y
                                    case 2: {
                                        double x = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + APointSerializerDeserializer
                                                        .getCoordinateOffset(Coordinate.X));
                                        value = x;
                                    }
                                        break;
                                    case 1:
                                    case 3: {
                                        double y = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + APointSerializerDeserializer
                                                        .getCoordinateOffset(Coordinate.Y));

                                        value = y;
                                    }
                                        break;
                                    default: {
                                        throw new NotImplementedException(
                                                coordinate + " is not a valid coordinate option");
                                    }
                                }
                                break;
                            case LINE:
                                value = Double.MAX_VALUE;
                                switch (coordinate) {
                                    case 0: {
                                        value = Double.MAX_VALUE;
                                        double startX = ADoubleSerializerDeserializer.getDouble(
                                                inputArg0.getByteArray(), startOffset0 + ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.X));
                                        double endX = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + ALineSerializerDeserializer
                                                        .getEndPointCoordinateOffset(Coordinate.X));

                                        value = Math.min(Math.min(startX, endX), value);
                                    }
                                        break;
                                    case 1: {
                                        value = Double.MAX_VALUE;
                                        double startY = ADoubleSerializerDeserializer.getDouble(
                                                inputArg0.getByteArray(), startOffset0 + ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.Y));
                                        double endY = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + ALineSerializerDeserializer
                                                        .getEndPointCoordinateOffset(Coordinate.Y));

                                        value = Math.min(Math.min(startY, endY), value);
                                    }
                                        break;
                                    case 2: {
                                        value = Double.MIN_VALUE;
                                        double startX = ADoubleSerializerDeserializer.getDouble(
                                                inputArg0.getByteArray(), startOffset0 + ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.X));
                                        double endX = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + ALineSerializerDeserializer
                                                        .getEndPointCoordinateOffset(Coordinate.X));

                                        value = Math.max(Math.min(startX, endX), value);
                                    }
                                        break;
                                    case 3: {
                                        value = Double.MIN_VALUE;
                                        double startY = ADoubleSerializerDeserializer.getDouble(
                                                inputArg0.getByteArray(), startOffset0 + ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.Y));
                                        double endY = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + ALineSerializerDeserializer
                                                        .getEndPointCoordinateOffset(Coordinate.Y));

                                        value = Math.max(Math.min(startY, endY), value);
                                    }
                                        break;
                                    default: {
                                        throw new NotImplementedException(
                                                coordinate + " is not a valid coordinate option");
                                    }
                                }
                                break;
                            case POLYGON:
                                int numOfPoints = AInt16SerializerDeserializer.getShort(inputArg0.getByteArray(),
                                        startOffset0 + APolygonSerializerDeserializer.getNumberOfPointsOffset());
                                switch (coordinate) {
                                    case 0: {
                                        value = Double.MAX_VALUE;
                                        for (int i = 0; i < numOfPoints; i++) {
                                            double x = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                    startOffset0 + APolygonSerializerDeserializer.getCoordinateOffset(i,
                                                            Coordinate.X));
                                            value = Math.min(x, value);
                                        }
                                    }
                                        break;
                                    case 1: {
                                        value = Double.MAX_VALUE;
                                        for (int i = 0; i < numOfPoints; i++) {
                                            double y = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                    startOffset0 + APolygonSerializerDeserializer.getCoordinateOffset(i,
                                                            Coordinate.Y));
                                            value = Math.min(y, value);
                                        }
                                    }
                                        break;
                                    case 2: {
                                        value = Double.MIN_VALUE;
                                        for (int i = 0; i < numOfPoints; i++) {
                                            double x = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                    startOffset0 + APolygonSerializerDeserializer.getCoordinateOffset(i,
                                                            Coordinate.X));
                                            value = Math.max(x, value);
                                        }
                                    }
                                        break;
                                    case 3: {
                                        value = Double.MIN_VALUE;
                                        for (int i = 0; i < numOfPoints; i++) {
                                            double y = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                    startOffset0 + APolygonSerializerDeserializer.getCoordinateOffset(i,
                                                            Coordinate.Y));
                                            value = Math.max(y, value);
                                        }
                                    }
                                        break;
                                    default: {
                                        throw new NotImplementedException(
                                                coordinate + " is not a valid coordinate option");
                                    }
                                }
                                break;
                            case CIRCLE:
                                switch (coordinate) {
                                    case 0: {
                                        double x = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + ACircleSerializerDeserializer
                                                        .getCenterPointCoordinateOffset(Coordinate.X));
                                        double radius = ADoubleSerializerDeserializer.getDouble(
                                                inputArg0.getByteArray(),
                                                startOffset0 + ACircleSerializerDeserializer.getRadiusOffset());
                                        value = x - radius;
                                    }
                                        break;
                                    case 1: {
                                        double y = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + ACircleSerializerDeserializer
                                                        .getCenterPointCoordinateOffset(Coordinate.Y));
                                        double radius = ADoubleSerializerDeserializer.getDouble(
                                                inputArg0.getByteArray(),
                                                startOffset0 + ACircleSerializerDeserializer.getRadiusOffset());

                                        value = y - radius;
                                    }
                                        break;
                                    case 2: {
                                        double x = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + ACircleSerializerDeserializer
                                                        .getCenterPointCoordinateOffset(Coordinate.X));
                                        double radius = ADoubleSerializerDeserializer.getDouble(
                                                inputArg0.getByteArray(),
                                                startOffset0 + ACircleSerializerDeserializer.getRadiusOffset());

                                        value = x + radius;
                                    }
                                        break;
                                    case 3: {
                                        double y = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + ACircleSerializerDeserializer
                                                        .getCenterPointCoordinateOffset(Coordinate.Y));
                                        double radius = ADoubleSerializerDeserializer.getDouble(
                                                inputArg0.getByteArray(),
                                                startOffset0 + ACircleSerializerDeserializer.getRadiusOffset());

                                        value = y + radius;
                                    }
                                        break;
                                    default: {
                                        throw new NotImplementedException(
                                                coordinate + " is not a valid coordinate option");
                                    }
                                }
                                break;
                            case RECTANGLE:
                                value = Double.MAX_VALUE;
                                switch (coordinate) {
                                    case 0: {
                                        value = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + ARectangleSerializerDeserializer
                                                        .getBottomLeftCoordinateOffset(Coordinate.X));
                                    }
                                        break;
                                    case 1: {
                                        value = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + ARectangleSerializerDeserializer
                                                        .getBottomLeftCoordinateOffset(Coordinate.Y));
                                    }
                                        break;
                                    case 2: {
                                        value = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + ARectangleSerializerDeserializer
                                                        .getUpperRightCoordinateOffset(Coordinate.X));
                                    }
                                        break;
                                    case 3: {
                                        value = ADoubleSerializerDeserializer.getDouble(inputArg0.getByteArray(),
                                                startOffset0 + ARectangleSerializerDeserializer
                                                        .getUpperRightCoordinateOffset(Coordinate.Y));
                                    }
                                        break;
                                    default: {
                                        throw new NotImplementedException(
                                                coordinate + " is not a valid coordinate option");
                                    }
                                }
                                break;
                            case NULL: {
                                out.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                                result.set(resultStorage);
                                return;
                            }
                            default:
                                throw new NotImplementedException(
                                        "create-mbr is only implemented for POINT, LINE, POLYGON, CIRCLE and RECTANGLE. Encountered type: "
                                                + tag + ".");

                        }
                    } else {
                        throw new NotImplementedException(dimension + "D is not supported");
                    }
                    out.writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                    out.writeDouble(value);
                } catch (HyracksDataException hde) {
                    throw new AlgebricksException(hde);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
                result.set(resultStorage);
            }
        };
    }
}
