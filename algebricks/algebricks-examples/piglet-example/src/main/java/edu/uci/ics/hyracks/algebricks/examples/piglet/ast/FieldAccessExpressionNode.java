package edu.uci.ics.hyracks.algebricks.examples.piglet.ast;

public class FieldAccessExpressionNode extends ExpressionNode {
    private String relationName;

    private String fieldName;

    public FieldAccessExpressionNode(String relationName, String fieldName) {
        this.relationName = relationName;
        this.fieldName = fieldName;
    }

    @Override
    public Tag getTag() {
        return Tag.FIELD_ACCESS;
    }

    public String getRelationName() {
        return relationName;
    }

    public String getFieldName() {
        return fieldName;
    }
}