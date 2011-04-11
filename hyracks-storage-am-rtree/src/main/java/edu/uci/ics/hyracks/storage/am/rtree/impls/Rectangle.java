package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class Rectangle {
    private int dim;
    private double[] low;
    private double[] high;

    public Rectangle(int dim) {
        this.dim = dim;
        low = new double[this.dim];
        high = new double[this.dim];
    }

    public int getDim() {
        return dim;
    }

    public double getLow(int i) {
        return low[i];
    }

    public double getHigh(int i) {
        return high[i];
    }

    public void setLow(int i, double value) {
        low[i] = value;
    }

    public void setHigh(int i, double value) {
        high[i] = value;
    }

    public void set(ITupleReference tuple) {
        for (int i = 0; i < getDim(); i++) {
            int j = i + getDim();
            setLow(i, DoubleSerializerDeserializer.getDouble(tuple.getFieldData(i), tuple.getFieldStart(i)));
            setHigh(i, DoubleSerializerDeserializer.getDouble(tuple.getFieldData(j), tuple.getFieldStart(j)));
        }
    }

    public void enlarge(ITupleReference tupleToBeInserted) {
        for (int i = 0; i < getDim(); i++) {
            int j = getDim() + i;
            double low = DoubleSerializerDeserializer.getDouble(tupleToBeInserted.getFieldData(i),
                    tupleToBeInserted.getFieldStart(i));
            if (getLow(i) > low) {
                setLow(i, low);
            }
            double high = DoubleSerializerDeserializer.getDouble(tupleToBeInserted.getFieldData(j),
                    tupleToBeInserted.getFieldStart(j));
            if (getHigh(i) < high) {
                setHigh(i, high);
            }
        }
    }

    public double margin() {
        double margin = 0.0;
        double mul = Math.pow(2, (double) getDim() - 1.0);
        for (int i = 0; i < getDim(); i++) {
            margin += (getHigh(i) - getLow(i)) * mul;
        }
        return margin;
    }
    
    public double overlappedArea(ITupleReference tuple) {
        double area = 1.0;
        double f1, f2;
        
        for (int i = 0; i < getDim(); i++)
        {
            int j = getDim() + i;
            double low = DoubleSerializerDeserializer.getDouble(tuple.getFieldData(i),
                    tuple.getFieldStart(i));
            double high = DoubleSerializerDeserializer.getDouble(tuple.getFieldData(j),
                    tuple.getFieldStart(j));
            if (getLow(i) > high || getHigh(i) < low) {
                return 0.0;
            }
            f1 = Math.max(getLow(i), low);
            f2 = Math.min(getHigh(i), high);
            area *= f2 - f1;
        }
        return area;
    }
    
    public double overlappedArea(Rectangle rec) {
        double area = 1.0;
        double f1, f2;
        
        for (int i = 0; i < getDim(); i++)
        {
            if (getLow(i) > rec.getHigh(i) || getHigh(i) < rec.getLow(i)) {
                return 0.0;
            }

            f1 = Math.max(getLow(i), rec.getLow(i));
            f2 = Math.min(getHigh(i), rec.getHigh(i));
            area *= f2 - f1;
        }
        return area;
    }
    
    public double area(ITupleReference tuple) {
        double area = 1.0;
        for (int i = 0; i < getDim(); i++) {
            int j = getDim() + i;
            area *= DoubleSerializerDeserializer.getDouble(tuple.getFieldData(j), tuple.getFieldStart(j))
                    - DoubleSerializerDeserializer.getDouble(tuple.getFieldData(i), tuple.getFieldStart(i));
        }
        return area;
    }
    
    public double area() {
        double area = 1.0;
        for (int i = 0; i < getDim(); i++) {
            area *= getHigh(i) - getLow(i);
        }
        return area;
    }
}
