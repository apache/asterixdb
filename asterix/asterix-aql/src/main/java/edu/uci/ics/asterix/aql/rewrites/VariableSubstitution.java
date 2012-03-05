package edu.uci.ics.asterix.aql.rewrites;

import edu.uci.ics.asterix.aql.expression.VarIdentifier;

public final class VariableSubstitution {
    private final VarIdentifier oldVar;
    private final VarIdentifier newVar;

    // private final int newVarId;

    public VariableSubstitution(VarIdentifier oldVar, VarIdentifier newVar) {
        this.oldVar = oldVar;
        this.newVar = newVar;
        // this.newVarId = newVarId;
    }

    public VarIdentifier getOldVar() {
        return oldVar;
    }

    public VarIdentifier getNewVar() {
        return newVar;
    }

}
