package edu.uci.ics.asterix.aql.base;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

public abstract class AbstractExpression implements Expression {
	protected List<IExpressionAnnotation> hints;
	
	public void addHint(IExpressionAnnotation hint) {
    	if (hints == null) {
    		hints = new ArrayList<IExpressionAnnotation>();
    	}
    	hints.add(hint);
    }
    
    public boolean hasHints() {
    	return hints != null;
    }
    
    public List<IExpressionAnnotation> getHints() {
    	return hints;
    }
}
