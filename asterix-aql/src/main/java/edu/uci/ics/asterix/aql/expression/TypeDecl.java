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
import edu.uci.ics.asterix.common.annotations.TypeDataGen;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class TypeDecl implements Statement {

    private final Identifier dataverseName;
    private final Identifier ident;
    private final TypeExpression typeDef;
    private final TypeDataGen datagenAnnotation;
    private final boolean ifNotExists;

    public TypeDecl(Identifier dataverseName, Identifier ident, TypeExpression typeDef, TypeDataGen datagen,
            boolean ifNotExists) {
        this.dataverseName = dataverseName;
        this.ident = ident;
        this.typeDef = typeDef;
        this.datagenAnnotation = datagen;
        this.ifNotExists = ifNotExists;
    }

    public TypeDecl(Identifier dataverse, Identifier ident, TypeExpression typeDef) {
        this(dataverse, ident, typeDef, null, false);
    }

    public Identifier getIdent() {
        return ident;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public TypeExpression getTypeDef() {
        return typeDef;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.TYPE_DECL;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitTypeDecl(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    public TypeDataGen getDatagenAnnotation() {
        return datagenAnnotation;
    }

}
