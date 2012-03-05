package edu.uci.ics.asterix.common.annotations;

public class TypeDataGen {
    private final boolean dgen;
    private final String outputFileName;
    private final long numValues;

    public TypeDataGen(boolean dgen, String outputFileName, long numValues) {
        this.dgen = dgen;
        this.outputFileName = outputFileName;
        this.numValues = numValues;
    }

    public boolean isDataGen() {
        return dgen;
    }

    public long getNumValues() {
        return numValues;
    }

    public String getOutputFileName() {
        return outputFileName;
    }
}
