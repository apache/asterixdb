package edu.uci.ics.hyracks.algebricks.examples.piglet.ast;

public class AssignmentNode extends ASTNode {
    private String alias;

    private RelationNode relation;

    public AssignmentNode(String alias, RelationNode relation) {
        this.alias = alias;
        this.relation = relation;
    }

    @Override
    public Tag getTag() {
        return Tag.ASSIGNMENT;
    }

    public String getAlias() {
        return alias;
    }

    public RelationNode getRelation() {
        return relation;
    }
}