package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.dataflow.data.nontagged.Coordinate;
import edu.uci.ics.asterix.om.base.APoint;
import edu.uci.ics.asterix.om.base.APolygon;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class APolygonSerializerDeserializer implements ISerializerDeserializer<APolygon> {

    private static final long serialVersionUID = 1L;

    public static final APolygonSerializerDeserializer INSTANCE = new APolygonSerializerDeserializer();

    private APolygonSerializerDeserializer() {
    }

    @Override
    public APolygon deserialize(DataInput in) throws HyracksDataException {
        try {
            short numberOfPoints = in.readShort();
            APoint[] points = new APoint[numberOfPoints];
            for (int i = 0; i < numberOfPoints; i++) {
                points[i] = APointSerializerDeserializer.INSTANCE.deserialize(in);
            }
            return new APolygon(points);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(APolygon instance, DataOutput out) throws HyracksDataException {
        try {
            int n = instance.getNumberOfPoints();
            out.writeShort(n);
            for (int i = 0; i < n; i++) {
                APointSerializerDeserializer.INSTANCE.serialize(instance.getPoints()[i], out);
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public final static int getNumberOfPointsOffset() throws HyracksDataException {
        return 1;
    }

    public final static int getCoordinateOffset(int pointId, Coordinate coordinate) throws HyracksDataException {
        switch (coordinate) {
            case X:
                return 3 + (pointId * 16);
            case Y:
                return 11 + (pointId * 16);
            default:
                throw new HyracksDataException("Wrong coordinate");
        }
    }

    public static void parse(String polygon, DataOutput out) throws HyracksDataException {
        try {
            String[] points = polygon.split(" ");
            if (points.length < 3)
                throw new HyracksDataException("Polygon must have at least 3 points.");
            out.writeByte(ATypeTag.POLYGON.serialize());
            out.writeShort(points.length);
            for (int i = 0; i < points.length; i++) {
                APointSerializerDeserializer.serialize(Double.parseDouble(points[i].split(",")[0]),
                        Double.parseDouble(points[i].split(",")[1]), out);
            }
        } catch (Exception e) {
            throw new HyracksDataException(polygon + " can not be an instance of polygon");
        }
    }
}
