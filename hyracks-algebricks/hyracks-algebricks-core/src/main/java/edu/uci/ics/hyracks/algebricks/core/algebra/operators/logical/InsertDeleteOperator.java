package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class InsertDeleteOperator extends AbstractLogicalOperator {

    public enum Kind {
        INSERT,
        DELETE
    }

    private final IDataSource<?> dataSource;
    private final Mutable<ILogicalExpression> payloadExpr;
    private final List<Mutable<ILogicalExpression>> primaryKeyExprs;
    private final Kind operation;

    public InsertDeleteOperator(IDataSource<?> dataSource, Mutable<ILogicalExpression> payload,
            List<Mutable<ILogicalExpression>> primaryKeyExprs, Kind operation) {
        this.dataSource = dataSource;
        this.payloadExpr = payload;
        this.primaryKeyExprs = primaryKeyExprs;
        this.operation = operation;
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        schema = new ArrayList<LogicalVariable>();
        schema.addAll(inputs.get(0).getValue().getSchema());
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        boolean b = false;
        b = visitor.transform(payloadExpr);
        for (int i = 0; i < primaryKeyExprs.size(); i++) {
            if (visitor.transform(primaryKeyExprs.get(i))) {
                b = true;
            }
        }
        return b;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitInsertDeleteOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.INSERT_DELETE;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createPropagatingAllInputsTypeEnvironment(ctx);
    }

    public List<Mutable<ILogicalExpression>> getPrimaryKeyExpressions() {
        return primaryKeyExprs;
    }

    public IDataSource<?> getDataSource() {
        return dataSource;
    }

    public Mutable<ILogicalExpression> getPayloadExpression() {
        return payloadExpr;
    }

    public Kind getOperation() {
        return operation;
    }

}
