/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
