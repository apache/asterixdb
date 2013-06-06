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

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class NodegroupDecl implements Statement {

    private Identifier nodegroupName;
    private List<Identifier> nodeControllerNames;
    private boolean ifNotExists;

    public NodegroupDecl(Identifier nodegroupName, List<Identifier> nodeControllerNames, boolean ifNotExists) {
        this.nodegroupName = nodegroupName;
        this.nodeControllerNames = nodeControllerNames;
        this.ifNotExists = ifNotExists;
    }

    public Identifier getNodegroupName() {
        return nodegroupName;
    }

    public List<Identifier> getNodeControllerNames() {
        return nodeControllerNames;
    }

    public void setNodeControllerNames(List<Identifier> nodeControllerNames) {
        this.nodeControllerNames = nodeControllerNames;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.NODEGROUP_DECL;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitNodegroupDecl(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }
}
