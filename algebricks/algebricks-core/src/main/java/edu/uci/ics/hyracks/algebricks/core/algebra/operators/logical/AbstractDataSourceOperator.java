package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;

import java.util.List;

public abstract class AbstractDataSourceOperator extends AbstractScanOperator {
    protected IDataSource<?> dataSource;

    public AbstractDataSourceOperator(List<LogicalVariable> variables, IDataSource<?> dataSource) {
        super(variables);
        this.dataSource = dataSource;
    }

    public IDataSource<?> getDataSource() {
        return dataSource;
    }
}
