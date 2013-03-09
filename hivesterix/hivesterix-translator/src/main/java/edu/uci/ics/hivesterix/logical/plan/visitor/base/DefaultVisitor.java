package edu.uci.ics.hivesterix.logical.plan.visitor.base;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hadoop.hive.ql.exec.CollectOperator;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;

/**
 * a default empty implementation of visitor
 * 
 * @author yingyib
 */
public class DefaultVisitor implements Visitor {

    @Override
    public Mutable<ILogicalOperator> visit(CollectOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(JoinOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(ExtractOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(MapJoinOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(SMBMapJoinOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    public Mutable<ILogicalOperator> visit(FileSinkOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    public Mutable<ILogicalOperator> visit(ReduceSinkOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(FilterOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(ForwardOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(GroupByOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(LateralViewForwardOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(LateralViewJoinOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(LimitOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(MapOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(ScriptOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(SelectOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(TableScanOperator hiveOperator,
            Mutable<ILogicalOperator> AlgebricksParentOperator, Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(UDTFOperator operator, Mutable<ILogicalOperator> AlgebricksParentOperator,
            Translator t) throws AlgebricksException {
        return null;
    }

    @Override
    public Mutable<ILogicalOperator> visit(UnionOperator operator, Mutable<ILogicalOperator> AlgebricksParentOperator,
            Translator t) throws AlgebricksException {
        return null;
    }
}
