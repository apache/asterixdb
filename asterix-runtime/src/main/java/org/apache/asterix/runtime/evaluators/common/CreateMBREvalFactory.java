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
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class CreateMBREvalFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory recordEvalFactory;
    private ICopyEvaluatorFactory dimensionEvalFactory;
    private ICopyEvaluatorFactory coordinateEvalFactory;

    public CreateMBREvalFactory(ICopyEvaluatorFactory recordEvalFactory, ICopyEvaluatorFactory dimensionEvalFactory,
            ICopyEvaluatorFactory coordinateEvalFactory) {
        this.recordEvalFactory = recordEvalFactory;
        this.dimensionEvalFactory = dimensionEvalFactory;
        this.coordinateEvalFactory = coordinateEvalFactory;
    }

    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new ICopyEvaluator() {

            private DataOutput out = output.getDataOutput();

            private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
            private ArrayBackedValueStorage outInput2 = new ArrayBackedValueStorage();

            private ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            private ICopyEvaluator eval1 = dimensionEvalFactory.createEvaluator(outInput1);
            private ICopyEvaluator eval2 = coordinateEvalFactory.createEvaluator(outInput2);

            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                outInput0.reset();
                eval0.evaluate(tuple);
                outInput1.reset();
                eval1.evaluate(tuple);
                outInput2.reset();
                eval2.evaluate(tuple);

                // type-check: (Point/Line/Polygon/Circle/Rectangle/Null, Int32, Int32)
                if (outInput1.getByteArray()[0] != ATypeTag.SERIALIZED_INT32_TYPE_TAG
                        || outInput2.getByteArray()[0] != ATypeTag.SERIALIZED_INT32_TYPE_TAG) {
                    throw new AlgebricksException(
                            "Expects Types: (Point/Line/Polygon/Circle/Rectangle/Null, Int32, Int32).");
                }

                try {

                    int dimension = AInt32SerializerDeserializer.getInt(outInput1.getByteArray(), 1);
                    int coordinate = AInt32SerializerDeserializer.getInt(outInput2.getByteArray(), 1);
                    double value;
                    if (dimension == 2) {
                        ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput0.getByteArray()[0]);
                        switch (tag) {
                            case POINT:
                                switch (coordinate) {
                                    case 0: // 0 is for min x, 1 is for min y, 2
                                           // for
                                           // max x, and 3 for max y
                                    case 2: {
                                        double x = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));

                                        value = x;
                                    }
                                        break;
                                    case 1:
                                    case 3: {
                                        double y = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

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
                                        double startX = ADoubleSerializerDeserializer
                                                .getDouble(outInput0.getByteArray(), ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.X));
                                        double endX = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.X));

                                        value = Math.min(Math.min(startX, endX), value);
                                    }
                                        break;
                                    case 1: {
                                        value = Double.MAX_VALUE;
                                        double startY = ADoubleSerializerDeserializer
                                                .getDouble(outInput0.getByteArray(), ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.Y));
                                        double endY = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.Y));

                                        value = Math.min(Math.min(startY, endY), value);
                                    }
                                        break;
                                    case 2: {
                                        value = Double.MIN_VALUE;
                                        double startX = ADoubleSerializerDeserializer
                                                .getDouble(outInput0.getByteArray(), ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.X));
                                        double endX = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.X));

                                        value = Math.max(Math.min(startX, endX), value);
                                    }
                                        break;
                                    case 3: {
                                        value = Double.MIN_VALUE;
                                        double startY = ADoubleSerializerDeserializer
                                                .getDouble(outInput0.getByteArray(), ALineSerializerDeserializer
                                                        .getStartPointCoordinateOffset(Coordinate.Y));
                                        double endY = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                ALineSerializerDeserializer.getEndPointCoordinateOffset(Coordinate.Y));

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
                                int numOfPoints = AInt16SerializerDeserializer.getShort(outInput0.getByteArray(),
                                        APolygonSerializerDeserializer.getNumberOfPointsOffset());
                                switch (coordinate) {
                                    case 0: {
                                        value = Double.MAX_VALUE;
                                        for (int i = 0; i < numOfPoints; i++) {
                                            double x = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                    APolygonSerializerDeserializer.getCoordinateOffset(i,
                                                            Coordinate.X));
                                            value = Math.min(x, value);
                                        }
                                    }
                                        break;
                                    case 1: {
                                        value = Double.MAX_VALUE;
                                        for (int i = 0; i < numOfPoints; i++) {
                                            double y = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                    APolygonSerializerDeserializer.getCoordinateOffset(i,
                                                            Coordinate.Y));
                                            value = Math.min(y, value);
                                        }
                                    }
                                        break;
                                    case 2: {
                                        value = Double.MIN_VALUE;
                                        for (int i = 0; i < numOfPoints; i++) {
                                            double x = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                    APolygonSerializerDeserializer.getCoordinateOffset(i,
                                                            Coordinate.X));
                                            value = Math.max(x, value);
                                        }
                                    }
                                        break;
                                    case 3: {
                                        value = Double.MIN_VALUE;
                                        for (int i = 0; i < numOfPoints; i++) {
                                            double y = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                    APolygonSerializerDeserializer.getCoordinateOffset(i,
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
                                        double x = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                ACircleSerializerDeserializer
                                                        .getCenterPointCoordinateOffset(Coordinate.X));
                                        double radius = ADoubleSerializerDeserializer.getDouble(
                                                outInput0.getByteArray(),
                                                ACircleSerializerDeserializer.getRadiusOffset());

                                        value = x - radius;
                                    }
                                        break;
                                    case 1: {
                                        double y = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                ACircleSerializerDeserializer
                                                        .getCenterPointCoordinateOffset(Coordinate.Y));
                                        double radius = ADoubleSerializerDeserializer.getDouble(
                                                outInput0.getByteArray(),
                                                ACircleSerializerDeserializer.getRadiusOffset());

                                        value = y - radius;
                                    }
                                        break;
                                    case 2: {
                                        double x = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                ACircleSerializerDeserializer
                                                        .getCenterPointCoordinateOffset(Coordinate.X));
                                        double radius = ADoubleSerializerDeserializer.getDouble(
                                                outInput0.getByteArray(),
                                                ACircleSerializerDeserializer.getRadiusOffset());

                                        value = x + radius;
                                    }
                                        break;
                                    case 3: {
                                        double y = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                ACircleSerializerDeserializer
                                                        .getCenterPointCoordinateOffset(Coordinate.Y));
                                        double radius = ADoubleSerializerDeserializer.getDouble(
                                                outInput0.getByteArray(),
                                                ACircleSerializerDeserializer.getRadiusOffset());

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
                                        value = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                ARectangleSerializerDeserializer
                                                        .getBottomLeftCoordinateOffset(Coordinate.X));
                                    }
                                        break;
                                    case 1: {
                                        value = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                ARectangleSerializerDeserializer
                                                        .getBottomLeftCoordinateOffset(Coordinate.Y));
                                    }
                                        break;
                                    case 2: {
                                        value = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                ARectangleSerializerDeserializer
                                                        .getUpperRightCoordinateOffset(Coordinate.X));
                                    }
                                        break;
                                    case 3: {
                                        value = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                                ARectangleSerializerDeserializer
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
            }
        };
    }
}
