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
package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class Query implements Statement {
	private Expression body;
	private int varCounter;

	public Expression getBody() {
		return body;
	}

	public void setBody(Expression body) {
		this.body = body;
	}

	public int getVarCounter() {
		return varCounter;
	}

	public void setVarCounter(int varCounter) {
		this.varCounter = varCounter;
	}

	@Override
	public Kind getKind() {
		return Kind.QUERY;
	}

	@Override
	public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T step)
			throws AsterixException {
		visitor.visit(this, step);
	}

	@Override
	public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg)
			throws AsterixException {
		return visitor.visitQuery(this, arg);
	}
}
