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
        return buildExpectedSchemaNodes(expr, typeEnv, producedVar);
    }

    private boolean setSchemaFromCalculatedExpression(AbstractFunctionCallExpression expr, LogicalVariable producedVar,
            IVariableTypeEnvironment typeEnv) throws AlgebricksException {
        //Parent always nested
        AbstractComplexExpectedSchemaNode parent = (AbstractComplexExpectedSchemaNode) buildNestedNode(expr, typeEnv);
        if (parent != null) {
            IExpectedSchemaNode leaf = new AnyExpectedSchemaNode(parent, expr);
            IExpectedSchemaNode oldChildNode = parent.getChildNode(expr);
            IExpectedSchemaNode actualNode = addOrReplaceChild(expr, typeEnv, parent, leaf);
            if (producedVar != null) {
                //Register the node if a variable is produced
                varToNode.put(producedVar, actualNode);
                updateVarToNodeRef(oldChildNode, actualNode);
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
            // Both expressions are null as they're the node isn't used anymore and the node ANY is not replaceable
            AnyExpectedSchemaNode leafNode =
                    (AnyExpectedSchemaNode) node.replaceIfNeeded(ExpectedSchemaNodeType.ANY, null, null);
            // make the leaf node irreplaceable
            leafNode.preventReplacing();
            // if the node has been replaced by the leafNode,
            // check for the variable that is associated with the node to be replaced.
            varToNode.put(variable, leafNode);
            updateVarToNodeRef(node, leafNode);
        }
    }

    /**
     * Updates variable references when schema paths point to the same field access.
     *
     * Example Query:
     * SELECT i
     * ... FROM orders AS o, o.items AS i
     * ... WHERE (SOME li IN o.items SATISFIES li.qty < 3)
     * ... AND i.qty >= 100;
     *
     * In this query, both 'i' and 'li' reference the same field access (o.items).
     * The schema paths evolve as follows:
     * 1. i.qty creates path: {"items": [{qty: any}]}
     * 2. li.qty references the same qty node
     * 3. When processing 'i', we replace i.qty with a broader path: {"items": [any]}
     *
     * Since both variables reference the same field, we need to update all references
     * to the old node (qty-specific) with the new node (array-level) in varToNode cache.
     *
     * @param oldNode The original node being replaced (e.g., qty-specific node)
     * @param newNode The new node replacing it (e.g., array-level node)
     */
    private void updateVarToNodeRef(IExpectedSchemaNode oldNode, IExpectedSchemaNode newNode) {
        // Skip if nodes are null, identical, or oldNode is a root node
        if (oldNode == null || oldNode == newNode || RootExpectedSchemaNode.isPreDefinedRootNode(oldNode)) {
            return;
        }

        // Update all variable references from oldNode to newNode
        varToNode.replaceAll((var, node) -> node == oldNode ? newNode : node);
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

    private boolean buildExpectedSchemaNodes(ILogicalExpression expr, IVariableTypeEnvironment typeEnv,
            LogicalVariable producedVar) throws AlgebricksException {
        return buildNestedNodes(expr, typeEnv, producedVar);
    }

    private boolean buildNestedNodes(ILogicalExpression expr, IVariableTypeEnvironment typeEnv,
            LogicalVariable producedVar) throws AlgebricksException {
        //The current node expression
        boolean changed = false;
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        AbstractFunctionCallExpression myExpr = (AbstractFunctionCallExpression) expr;
        if (!SUPPORTED_FUNCTIONS.contains(myExpr.getFunctionIdentifier()) || noArgsOrFirstArgIsConstant(myExpr)) {
            // Check if the function consists of the Supported Functions
            for (Mutable<ILogicalExpression> arg : myExpr.getArguments()) {
                changed |= buildNestedNodes(arg.getValue(), typeEnv, producedVar);
            }

            return changed;
        }

        // if the child is not a function expression, then just one node.
        if (BuiltinFunctions.ARRAY_STAR.equals(myExpr.getFunctionIdentifier())
                || BuiltinFunctions.SCAN_COLLECTION.equals(myExpr.getFunctionIdentifier())) {
            // these supported function won't have second child
            IExpectedSchemaNode expectedSchemaNode = buildNestedNode(expr, typeEnv);
            if (expectedSchemaNode != null) {
                changed |=
                        setSchemaFromCalculatedExpression((AbstractFunctionCallExpression) expr, producedVar, typeEnv);
            }
        } else {
            ILogicalExpression childExpr = myExpr.getArguments().get(1).getValue();
            if (childExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                // must be a variable or constant
                IExpectedSchemaNode expectedSchemaNode = buildNestedNode(expr, typeEnv);
                if (expectedSchemaNode != null) {
                    changed |= setSchemaFromCalculatedExpression((AbstractFunctionCallExpression) expr, producedVar,
                            typeEnv);
                }
            } else {
                // as the childExpr is a function.
                // if the function had been evaluated at compile time, it would have been
                // evaluated at this stage of compilation.
                // eg: field-access(t.r.p, substring("name",2,4))
                // this will be evaluated to field-access(t.r.p, "me") at compile time itself.

                // since the execution reached this branch, this means the childExpr
                // need to be evaluated at runtime, hence the childExpr should also be checked
                // for possible pushdown.
                // eg: field-access(t.r.p, substring(x.y.age_field, 0, 4))
                ILogicalExpression parentExpr = myExpr.getArguments().get(0).getValue();
                IExpectedSchemaNode parentExpectedNode = buildNestedNode(parentExpr, typeEnv);
                if (parentExpectedNode != null) {
                    changed |= setSchemaFromCalculatedExpression((AbstractFunctionCallExpression) parentExpr,
                            producedVar, typeEnv);
                }
                changed |= buildNestedNodes(childExpr, typeEnv, producedVar);
            }
        }
        return changed;
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
            return changeNodeForVariable(sourceVar, myExpr, myExpr);
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
            AbstractComplexExpectedSchemaNode myNode =
                    AbstractComplexExpectedSchemaNode.createNestedNode(myType, newParent, parentFuncExpr, myExpr);
            // Add (or replace old child with) myNode to the parent
            return addOrReplaceChild(parentFuncExpr, typeEnv, newParent, myNode);
        }
        return null;
    }

    private boolean noArgsOrFirstArgIsConstant(AbstractFunctionCallExpression myExpr) {
        List<Mutable<ILogicalExpression>> args = myExpr.getArguments();
        return args.isEmpty() || args.get(0).getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT;
    }

    private IExpectedSchemaNode changeNodeForVariable(LogicalVariable sourceVar,
            AbstractFunctionCallExpression parentExpression, ILogicalExpression expression) {
        //Get the associated node with the sourceVar (if any)
        IExpectedSchemaNode oldNode = varToNode.get(sourceVar);
        if (oldNode == null || !oldNode.allowsReplacing()) {
            // Variable is not associated with a node. No pushdown is possible
            // Or its associated node cannot be replaced
            return null;
        }
        //What is the expected type of the variable
        ExpectedSchemaNodeType varExpectedType = getExpectedNestedNodeType(parentExpression);
        // Get the node associated with the variable (or change its type if needed).
        IExpectedSchemaNode newNode = oldNode.replaceIfNeeded(varExpectedType, parentExpression, expression);
        //Map the sourceVar to the node
        varToNode.put(sourceVar, newNode);
        updateVarToNodeRef(oldNode, newNode);
        return newNode;
    }

    public static IExpectedSchemaNode addOrReplaceChild(AbstractFunctionCallExpression parentExpr,
            IVariableTypeEnvironment typeEnv, AbstractComplexExpectedSchemaNode parent, IExpectedSchemaNode child)
            throws AlgebricksException {
        switch (parent.getType()) {
            case OBJECT:
                return handleObject(parentExpr, typeEnv, parent, child);
            case ARRAY:
                return handleArray(parent, child);
            case UNION:
                return handleUnion(parentExpr, parent, child);
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

    private static IExpectedSchemaNode handleObject(AbstractFunctionCallExpression parentExpr,
            IVariableTypeEnvironment typeEnv, AbstractComplexExpectedSchemaNode parent, IExpectedSchemaNode child)
            throws AlgebricksException {
        int fieldNameId = PushdownUtil.getFieldNameId(parentExpr);
        String fieldName = PushdownUtil.getFieldName(parentExpr, typeEnv);
        ObjectExpectedSchemaNode objectNode = (ObjectExpectedSchemaNode) parent;
        IExpectedSchemaNode actualChild = objectNode.getChildren().get(fieldName);
        if (actualChild == null) {
            objectNode.addChild(fieldName, fieldNameId, child);
            actualChild = child;
        } else {
            actualChild = objectNode.replaceChild(actualChild, child);
        }

        return actualChild;
    }

    private static IExpectedSchemaNode handleArray(AbstractComplexExpectedSchemaNode parent,
            IExpectedSchemaNode child) {
        ArrayExpectedSchemaNode arrayNode = (ArrayExpectedSchemaNode) parent;
        IExpectedSchemaNode actualChild = arrayNode.getChild();
        if (actualChild == null) {
            arrayNode.addChild(child);
            actualChild = child;
        } else {
            actualChild = arrayNode.replaceChild(actualChild, child);
        }

        return actualChild;
    }

    private static IExpectedSchemaNode handleUnion(AbstractFunctionCallExpression parentExpr,
            AbstractComplexExpectedSchemaNode parent, IExpectedSchemaNode child) throws AlgebricksException {
        UnionExpectedSchemaNode unionNode = (UnionExpectedSchemaNode) parent;
        ExpectedSchemaNodeType parentType = getExpectedNestedNodeType(parentExpr);
        AbstractComplexExpectedSchemaNode actualParent = unionNode.getChild(parentType);
        child.setParent(actualParent);
        return addOrReplaceChild(parentExpr, null, actualParent, child);
    }

    private static boolean isVariable(ILogicalExpression expr) {
        return expr.getExpressionTag() == LogicalExpressionTag.VARIABLE;
    }
}
