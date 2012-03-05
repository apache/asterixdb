package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class NodeGroupDropStatement implements Statement {

    private Identifier nodeGroupName;
    private boolean ifExists;

    public NodeGroupDropStatement(Identifier nodeGroupName, boolean ifExists) {
        this.nodeGroupName = nodeGroupName;
        this.ifExists = ifExists;
    }

    @Override
    public Kind getKind() {
        return Kind.NODEGROUP_DROP;
    }

    public Identifier getNodeGroupName() {
        return nodeGroupName;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitNodeGroupDropStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
