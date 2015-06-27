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

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

public abstract class CreateFeedStatement implements Statement {

    private final Pair<Identifier, Identifier> qName;
    private final FunctionSignature appliedFunction;
    private final boolean ifNotExists;

    public CreateFeedStatement(Pair<Identifier, Identifier> qName, FunctionSignature appliedFunction,
            boolean ifNotExists) {
        this.qName = qName;
        this.appliedFunction = appliedFunction;
        this.ifNotExists = ifNotExists;
    }

    public Identifier getDataverseName() {
        return qName.first;
    }

    public Identifier getFeedName() {
        return qName.second;
    }

    public FunctionSignature getAppliedFunction() {
        return appliedFunction;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public abstract Kind getKind();

    @Override
    public abstract <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException;

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
