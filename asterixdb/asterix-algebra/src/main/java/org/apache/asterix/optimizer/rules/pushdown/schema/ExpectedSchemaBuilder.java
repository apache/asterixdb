/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.optimizer.rules.pushdown.schema;

import static org.apache.asterix.metadata.utils.PushdownUtil.ARRAY_FUNCTIONS;
import static org.apache.asterix.metadata.utils.PushdownUtil.SUPPORTED_FUNCTIONS;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.metadata.utils.PushdownUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

/**
 * This class takes a value access expression and produces an expected schema (given the expression).
 * Example:
 * - $$t.getField("hashtags").getItem(0)
 * We expect:
 * 1- $$t is OBJECT
 * 2- the output type of getField("hashtags") is ARRAY
 * 3- the output type of getItem(0) is ANY node
 */
public class ExpectedSchemaBuilder {
    //Registered Variables
    private final Map<LogicalVariable, IExpectedSchemaNode> varToNode;

    public ExpectedSchemaBuilder() {
        varToNode = new HashMap<>();
    }

    public boolean setSchemaFromExpression(AbstractFunctionCallExpression expr, LogicalVariable producedVar,
            IVariableTypeEnvironment typeEnv) throws AlgebricksException {
        //Parent always nested
        AbstractComplexExpectedSchemaNode parent = (AbstractComplexExpectedSchemaNode) buildNestedNode(expr, typeEnv);
        if (parent != null) {
            IExpectedSchemaNode leaf =
                    new AnyExpectedSchemaNode(parent, expr.getSourceLocation(), expr.getFunctionIdentifier().getName());
            addChild(expr, typeEnv, parent, leaf);
            if (producedVar != null) {
                //Register the node if a variable is produced
                varToNode.put(producedVar, leaf);
            }
        }
        return parent != null;
    }

    public void registerRoot(LogicalVariable recordVar, RootExpectedSchemaNode rootNode) {
        varToNode.put(recordVar, rootNode);
    }

    public void unregisterVariable(LogicalVariable variable) {
        //Remove the node so no other expression will pushdown any expression in the future
        IExpectedSchemaNode node = varToNode.remove(variable);
        AbstractComplexExpectedSchemaNode parent = node.getParent();
        if (parent == null) {
            //It is a root node. Request the entire record
            varToNode.put(variable, RootExpectedSchemaNode.ALL_FIELDS_ROOT_IRREPLACEABLE_NODE);
        } else {
            // If it is a nested node, replace it to a LEAF node
            AnyExpectedSchemaNode leafNode = (AnyExpectedSchemaNode) node.replaceIfNeeded(ExpectedSchemaNodeType.ANY,
                    parent.getSourceLocation(), parent.getFunctionName());
            // make the leaf node irreplaceable
            leafNode.preventReplacing();
            varToNode.put(variable, leafNode);
        }
    }

    public boolean isVariableRegistered(LogicalVariable variable) {
        return varToNode.containsKey(variable);
    }

    public boolean isEmpty() {
        return varToNode.isEmpty();
    }

    public IExpectedSchemaNode getNode(LogicalVariable variable) {
        return varToNode.get(variable);
    }

    private IExpectedSchemaNode buildNestedNode(ILogicalExpression expr, IVariableTypeEnvironment typeEnv)
            throws AlgebricksException {
        //The current node expression
        AbstractFunctionCallExpression myExpr = (AbstractFunctionCallExpression) expr;
        if (!SUPPORTED_FUNCTIONS.contains(myExpr.getFunctionIdentifier()) || noArgsOrFirstArgIsConstant(myExpr)) {
            //Return null if the function is not supported.
            return null;
        }

        //The parent expression
        ILogicalExpression parentExpr = myExpr.getArguments().get(0).getValue();
        if (isVariable(parentExpr)) {
            //A variable could be the record's originated from data-scan or an expression from assign
            LogicalVariable sourceVar = VariableUtilities.getVariable(parentExpr);
            return changeNodeForVariable(sourceVar, myExpr);
        }

        //Recursively create the parent nodes. Parent is always a nested node
        AbstractComplexExpectedSchemaNode newParent =
                (AbstractComplexExpectedSchemaNode) buildNestedNode(parentExpr, typeEnv);
        //newParent could be null if the expression is not supported
        if (newParent != null) {
            //Parent expression must be a function call (as parent is a nested node)
            AbstractFunctionCallExpression parentFuncExpr = (AbstractFunctionCallExpression) parentExpr;
            //Get 'myType' as we will create the child type of the newParent
            ExpectedSchemaNodeType myType = getExpectedNestedNodeType(myExpr);
            /*
             * Create 'myNode'. It is a nested node because the function is either getField() or a supported array
             * function
             */
            AbstractComplexExpectedSchemaNode myNode = AbstractComplexExpectedSchemaNode.createNestedNode(myType,
                    newParent, myExpr.getSourceLocation(), myExpr.getFunctionIdentifier().getName());
            //Add myNode to the parent
            addChild(parentFuncExpr, typeEnv, newParent, myNode);
            return myNode;
        }
        return null;
    }

    private boolean noArgsOrFirstArgIsConstant(AbstractFunctionCallExpression myExpr) {
        List<Mutable<ILogicalExpression>> args = myExpr.getArguments();
        return args.isEmpty() || args.get(0).getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT;
    }

    private IExpectedSchemaNode changeNodeForVariable(LogicalVariable sourceVar,
            AbstractFunctionCallExpression myExpr) {
        //Get the associated node with the sourceVar (if any)
        IExpectedSchemaNode oldNode = varToNode.get(sourceVar);
        if (oldNode == null || !oldNode.allowsReplacing()) {
            // Variable is not associated with a node. No pushdown is possible
            // Or its associated node cannot be replaced
            return null;
        }
        //What is the expected type of the variable
        ExpectedSchemaNodeType varExpectedType = getExpectedNestedNodeType(myExpr);
        // Get the node associated with the variable (or change its type if needed).
        IExpectedSchemaNode newNode = oldNode.replaceIfNeeded(varExpectedType, myExpr.getSourceLocation(),
                myExpr.getFunctionIdentifier().getName());
        //Map the sourceVar to the node
        varToNode.put(sourceVar, newNode);
        return newNode;
    }

    public static void addChild(AbstractFunctionCallExpression parentExpr, IVariableTypeEnvironment typeEnv,
            AbstractComplexExpectedSchemaNode parent, IExpectedSchemaNode child) throws AlgebricksException {
        switch (parent.getType()) {
            case OBJECT:
                handleObject(parentExpr, typeEnv, parent, child);
                break;
            case ARRAY:
                handleArray(parent, child);
                break;
            case UNION:
                handleUnion(parentExpr, parent, child);
                break;
            default:
                throw new IllegalStateException("Node " + parent.getType() + " is not nested");

        }
    }

    public static ExpectedSchemaNodeType getExpectedNestedNodeType(AbstractFunctionCallExpression funcExpr) {
        FunctionIdentifier fid = funcExpr.getFunctionIdentifier();
        if (BuiltinFunctions.FIELD_ACCESS_BY_NAME.equals(fid) || BuiltinFunctions.FIELD_ACCESS_BY_INDEX.equals(fid)) {
            return ExpectedSchemaNodeType.OBJECT;
        } else if (ARRAY_FUNCTIONS.contains(fid)) {
            return ExpectedSchemaNodeType.ARRAY;
        }
        throw new IllegalStateException("Function " + fid + " should not be pushed down");
    }

    private static void handleObject(AbstractFunctionCallExpression parentExpr, IVariableTypeEnvironment typeEnv,
            AbstractComplexExpectedSchemaNode parent, IExpectedSchemaNode child) throws AlgebricksException {
        String fieldName = PushdownUtil.getFieldName(parentExpr, typeEnv);
        ObjectExpectedSchemaNode objectNode = (ObjectExpectedSchemaNode) parent;
        objectNode.addChild(fieldName, child);
    }

    private static void handleArray(AbstractComplexExpectedSchemaNode parent, IExpectedSchemaNode child) {
        ArrayExpectedSchemaNode arrayNode = (ArrayExpectedSchemaNode) parent;
        arrayNode.addChild(child);
    }

    private static void handleUnion(AbstractFunctionCallExpression parentExpr, AbstractComplexExpectedSchemaNode parent,
            IExpectedSchemaNode child) throws AlgebricksException {
        UnionExpectedSchemaNode unionNode = (UnionExpectedSchemaNode) parent;
        ExpectedSchemaNodeType parentType = getExpectedNestedNodeType(parentExpr);
        AbstractComplexExpectedSchemaNode actualParent = unionNode.getChild(parentType);
        child.setParent(actualParent);
        addChild(parentExpr, null, actualParent, child);
    }

    private static boolean isVariable(ILogicalExpression expr) {
        return expr.getExpressionTag() == LogicalExpressionTag.VARIABLE;
    }
}
