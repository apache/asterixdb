package edu.uci.ics.hyracks.storage.am.rtree.impls;


public class SpatialUtils {
    private double doubleEpsilon;

    public SpatialUtils() {
        double temp = 0.5;
        while (1 + temp > 1) {
            temp /= 2;
        }
        this.doubleEpsilon = temp;
    }
    
    public double getDoubleEpsilon() {
        return doubleEpsilon;
    }
}
