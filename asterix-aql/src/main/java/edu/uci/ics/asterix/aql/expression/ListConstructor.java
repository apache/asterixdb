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

import java.util.List;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class ListConstructor implements Expression {
    private List<Expression> exprList;
    private Type type;

    public ListConstructor() {
    }

    public ListConstructor(Type type, List<Expression> exprList) {
        this.type = type;
        this.exprList = exprList;
    }

    public List<Expression> getExprList() {
        return exprList;
    }

    public void setExprList(List<Expression> exprList) {
        this.exprList = exprList;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    @Override
    public Kind getKind() {
        return Kind.LIST_CONSTRUCTOR_EXPRESSION;
    }

    public enum Type {
        ORDERED_LIST_CONSTRUCTOR,
        UNORDERED_LIST_CONSTRUCTOR
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);

    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitListConstructor(this, arg);
    }
}
