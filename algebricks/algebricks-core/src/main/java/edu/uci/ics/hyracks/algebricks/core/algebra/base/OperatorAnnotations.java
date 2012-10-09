package edu.uci.ics.hyracks.algebricks.core.algebra.base;

public interface OperatorAnnotations {
    // hints
    public static final String USE_HASH_GROUP_BY = "USE_HASH_GROUP_BY"; // -->
    public static final String USE_EXTERNAL_GROUP_BY = "USE_EXTERNAL_GROUP_BY"; // -->
    // Boolean
    public static final String CARDINALITY = "CARDINALITY"; // -->
    // Integer
    public static final String MAX_NUMBER_FRAMES = "MAX_NUMBER_FRAMES"; // -->
    // Integer
}
