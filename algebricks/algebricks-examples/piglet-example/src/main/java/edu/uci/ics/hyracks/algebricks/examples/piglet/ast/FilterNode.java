package edu.uci.ics.hyracks.algebricks.examples.piglet.ast;

public class FilterNode extends RelationNode {
    private String alias;

    private ExpressionNode expression;

    public FilterNode(String alias, ExpressionNode expression) {
        this.alias = alias;
        this.expression = expression;
    }

    @Override
    public Tag getTag() {
        return Tag.FILTER;
    }

    public String getAlias() {
        return alias;
    }

    public ExpressionNode getExpression() {
        return expression;
    }
}