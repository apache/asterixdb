package edu.uci.ics.asterix.om.functions;


public class AsterixFunctionIdentifier {

    private final String dataverse;
    private final AsterixFunction asterixFunction;

    public AsterixFunctionIdentifier(String dataverse, AsterixFunction asterixFunction) {
        this.dataverse = dataverse;
        this.asterixFunction = asterixFunction;
    }

    public AsterixFunctionIdentifier(String dataverse, String name, int arity) {
        this.dataverse = dataverse;
        this.asterixFunction = new AsterixFunction(name, arity);
    }

    public String toString() {
        return dataverse + ":" + asterixFunction;
    }

    public String getDataverse() {
        return dataverse;
    }

    public AsterixFunction getAsterixFunction() {
        return asterixFunction;
    }

}
