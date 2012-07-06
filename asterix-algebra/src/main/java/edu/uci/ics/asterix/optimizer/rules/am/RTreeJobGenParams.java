package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl.IndexKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

/**
 * Helper class for reading and writing job-gen parameters for RTree access methods to
 * and from a list of function arguments, typically of an unnest-map.
 */
public class RTreeJobGenParams extends AccessMethodJobGenParams {
    
    protected List<LogicalVariable> keyVarList;
    
    public RTreeJobGenParams() {
    }
    
    public RTreeJobGenParams(String indexName, IndexKind indexKind, String datasetName, boolean retainInput, boolean requiresBroadcast) {
        this.indexName = indexName;
        this.indexKind = indexKind;
        this.datasetName = datasetName;
        this.retainInput = retainInput;
        this.requiresBroadcast = requiresBroadcast;
    }
    
    public void writeToFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.writeToFuncArgs(funcArgs);
        writeVarList(keyVarList, funcArgs);
    }
    
    public void readFromFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.readFromFuncArgs(funcArgs);
        int index = super.getNumParams();
        keyVarList = new ArrayList<LogicalVariable>();
        readVarList(funcArgs, index, keyVarList);
    }
    
    public void setKeyVarList(List<LogicalVariable> keyVarList) {
        this.keyVarList = keyVarList;
    }
    
    public List<LogicalVariable> getKeyVarList() {
        return keyVarList;
    }
}
