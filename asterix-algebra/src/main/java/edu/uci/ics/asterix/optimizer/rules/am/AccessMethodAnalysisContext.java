package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;

/**
 * Context for analyzing the applicability of a single access method.
 */
public class AccessMethodAnalysisContext {
    
    public List<IOptimizableFuncExpr> matchedFuncExprs = new ArrayList<IOptimizableFuncExpr>();
    
    // Contains candidate indexes and a list of integers that index into matchedFuncExprs.
    // We are mapping from candidate indexes to a list of function expressions 
    // that match one of the index's expressions.
    public HashMap<AqlCompiledIndexDecl, List<Integer>> indexExprs = new HashMap<AqlCompiledIndexDecl, List<Integer>>();
    
    // Maps from index to the dataset it is indexing.
    public HashMap<AqlCompiledIndexDecl, AqlCompiledDatasetDecl> indexDatasetMap = new HashMap<AqlCompiledIndexDecl, AqlCompiledDatasetDecl>();
    
    public void addIndexExpr(AqlCompiledDatasetDecl dataset,  AqlCompiledIndexDecl index, Integer exprIndex) {
    	List<Integer> exprs = indexExprs.get(index);
    	if (exprs == null) {
    	    exprs = new ArrayList<Integer>();
    		indexExprs.put(index, exprs);
    	}
    	exprs.add(exprIndex);
    	indexDatasetMap.put(index, dataset);
    }
    
    public List<Integer> getIndexExprs(AqlCompiledIndexDecl index) {
        return indexExprs.get(index);
    }
}
