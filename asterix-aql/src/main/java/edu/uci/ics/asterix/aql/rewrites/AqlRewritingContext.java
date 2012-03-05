package edu.uci.ics.asterix.aql.rewrites;

import java.util.HashMap;

import edu.uci.ics.asterix.aql.expression.VarIdentifier;

public final class AqlRewritingContext {
    private int varCounter;
    private HashMap<Integer, VarIdentifier> oldVarIdToNewVarId = new HashMap<Integer, VarIdentifier>();

    public AqlRewritingContext(int varCounter) {
        this.varCounter = varCounter;
    }

    public int getVarCounter() {
        return varCounter;
    }

    public VarIdentifier mapOldId(Integer oldId, String varValue) {
        int n = newId();
        VarIdentifier newVar = new VarIdentifier(varValue);
        newVar.setId(n);
        oldVarIdToNewVarId.put(oldId, newVar);
        return newVar;
    }

    public VarIdentifier mapOldVarIdentifier(VarIdentifier vi) {
        return mapOldId(vi.getId(), vi.getValue());
    }

    public VarIdentifier getRewrittenVar(Integer oldId) {
        return oldVarIdToNewVarId.get(oldId);
    }

    public VarIdentifier newVariable() {
        int id = newId();
        return new VarIdentifier("@@" + id, id);
    }

    private int newId() {
        varCounter++;
        return varCounter;
    }
}
