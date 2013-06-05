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
package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.dataflow.data.nontagged.Coordinate;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutablePoint;
import edu.uci.ics.asterix.om.base.AMutableRectangle;
import edu.uci.ics.asterix.om.base.APoint;
import edu.uci.ics.asterix.om.base.ARectangle;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ARectangleSerializerDeserializer implements ISerializerDeserializer<ARectangle> {

    private static final long serialVersionUID = 1L;

    public static final ARectangleSerializerDeserializer INSTANCE = new ARectangleSerializerDeserializer();

    @SuppressWarnings("unchecked")
    private final static ISerializerDeserializer<ARectangle> rectangleSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ARECTANGLE);
    private final static AMutableRectangle aRectangle = new AMutableRectangle(null, null);
    private final static AMutablePoint aRectanglePoint1 = new AMutablePoint(0, 0);
    private final static AMutablePoint aRectanglePoint2 = new AMutablePoint(0, 0);

    private ARectangleSerializerDeserializer() {
    }

    @Override
    public ARectangle deserialize(DataInput in) throws HyracksDataException {
        try {
            APoint p1 = APointSerializerDeserializer.INSTANCE.deserialize(in);
            APoint p2 = APointSerializerDeserializer.INSTANCE.deserialize(in);
            return new ARectangle(p1, p2);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ARectangle instance, DataOutput out) throws HyracksDataException {
        try {
            out.writeDouble(instance.getP1().getX());
            out.writeDouble(instance.getP1().getY());
            out.writeDouble(instance.getP2().getX());
            out.writeDouble(instance.getP2().getY());
        } catch (IOException e) {
            throw new HyracksDataException();
        }
    }

    public final static int getBottomLeftCoordinateOffset(Coordinate coordinate) throws HyracksDataException {

        switch (coordinate) {
            case X:
                return 1;
            case Y:
                return 9;
            default:
                throw new HyracksDataException("Wrong coordinate");
        }
    }

    public final static int getUpperRightCoordinateOffset(Coordinate coordinate) throws HyracksDataException {

        switch (coordinate) {
            case X:
                return 17;
            case Y:
                return 25;
            default:
                throw new HyracksDataException("Wrong coordinate");
        }
    }

    public static void parse(String rectangle, DataOutput out) throws HyracksDataException {
        try {
            String[] points = rectangle.split(" ");
            if (points.length != 2)
                throw new HyracksDataException("rectangle consists of only 2 points.");
            aRectanglePoint1.setValue(Double.parseDouble(points[0].split(",")[0]),
                    Double.parseDouble(points[0].split(",")[1]));
            aRectanglePoint2.setValue(Double.parseDouble(points[1].split(",")[0]),
                    Double.parseDouble(points[1].split(",")[1]));
            if (aRectanglePoint1.getX() > aRectanglePoint2.getX() && aRectanglePoint1.getY() > aRectanglePoint2.getY()) {
                aRectangle.setValue(aRectanglePoint2, aRectanglePoint1);
            } else if (aRectanglePoint1.getX() < aRectanglePoint2.getX() && aRectanglePoint1.getY() < aRectanglePoint2.getY()) {
                aRectangle.setValue(aRectanglePoint1, aRectanglePoint2);
            } else {
                throw new IllegalArgumentException(
                        "Rectangle arugment must be either (bottom left point, top right point) or (top right point, bottom left point)");
            }
            rectangleSerde.serialize(aRectangle, out);
        } catch (HyracksDataException e) {
            throw new HyracksDataException(rectangle + " can not be an instance of rectangle");
        }
    }
}
