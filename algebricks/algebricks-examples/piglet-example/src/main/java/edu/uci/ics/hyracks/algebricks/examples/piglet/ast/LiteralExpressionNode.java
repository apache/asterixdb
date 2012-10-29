package edu.uci.ics.hyracks.algebricks.examples.piglet.ast;

import edu.uci.ics.hyracks.algebricks.examples.piglet.types.Type;

public class LiteralExpressionNode extends ExpressionNode {
    private String image;

    private Type type;

    public LiteralExpressionNode(String image, Type type) {
        this.image = image;
        this.type = type;
    }

    @Override
    public Tag getTag() {
        return Tag.LITERAL;
    }

    public String getImage() {
        return image;
    }

    public Type getType() {
        return type;
    }
}