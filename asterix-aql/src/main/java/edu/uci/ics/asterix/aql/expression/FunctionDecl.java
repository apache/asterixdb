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
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;

public class FunctionDecl implements Statement {
    private FunctionSignature signature;
    private List<VarIdentifier> paramList;
    private Expression funcBody;

    public FunctionDecl(FunctionSignature signature, List<VarIdentifier> paramList, Expression funcBody) {
        this.signature = signature;
        this.paramList = paramList;
        this.funcBody = funcBody;
    }

    public FunctionSignature getSignature() {
        return signature;
    }

    public List<VarIdentifier> getParamList() {
        return paramList;
    }

    public Expression getFuncBody() {
        return funcBody;
    }

    public void setFuncBody(Expression funcBody) {
        this.funcBody = funcBody;
    }

    public void setSignature(FunctionSignature signature) {
        this.signature = signature;
    }

    public void setParamList(List<VarIdentifier> paramList) {
        this.paramList = paramList;
    }

    @Override
    public int hashCode() {
        return signature.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof FunctionDecl && ((FunctionDecl) o).getSignature().equals(signature));
    }

    @Override
    public Kind getKind() {
        return Kind.FUNCTION_DECL;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitFunctionDecl(this, arg);
    }
}
