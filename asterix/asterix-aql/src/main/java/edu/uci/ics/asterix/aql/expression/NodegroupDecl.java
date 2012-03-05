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
