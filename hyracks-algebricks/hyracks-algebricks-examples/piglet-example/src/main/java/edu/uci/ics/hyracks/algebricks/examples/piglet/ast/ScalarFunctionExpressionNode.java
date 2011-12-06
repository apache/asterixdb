package edu.uci.ics.hyracks.algebricks.examples.piglet.ast;

import java.util.List;

public class ScalarFunctionExpressionNode extends ExpressionNode {
    private FunctionTag fTag;

    private String fName;

    private List<ASTNode> arguments;

    public ScalarFunctionExpressionNode(FunctionTag fTag, String fName, List<ASTNode> arguments) {
        this.fTag = fTag;
        this.fName = fName;
        this.arguments = arguments;
    }

    @Override
    public Tag getTag() {
        return Tag.SCALAR_FUNCTION;
    }

    public FunctionTag getFunctionTag() {
        return fTag;
    }

    public String getFunctionName() {
        return fName;
    }

    public List<ASTNode> getArguments() {
        return arguments;
    }
}